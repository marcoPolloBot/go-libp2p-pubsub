# Topic Streams Extension — Implementation Plan

This document is a plan for implementing the **Topic Streams** gossipsub
extension in `go-libp2p-pubsub`.

Spec: `libp2p/specs`, branch `marco/topic-message-streams`,
`pubsub/gossipsub/topic-streams.md` (Working Draft, r0 2026-06-29). Companion
specs: `gossipsub-v1.3.md` (Extensions Control Message) and
`extensions/extensions.proto`.

---

## 1. What the spec requires

Gossipsub v1.3 multiplexes every RPC for every topic onto a **single stream per
direction**. This causes:

- **Head-of-line (HOL) blocking** — a large message on one topic delays small
  messages on other topics.
- **Per-message topic overhead** — every `Message` carries its full `topic`
  string.

The Topic Streams extension moves *topic-scoped application messages* off the
shared stream and onto **dedicated, long-lived streams per topic**.

Key requirements (from `topic-streams.md`):

1. **Negotiation** — both peers MUST advertise `topicStreams` in their
   `ControlExtensions` message. The extensions message is the first message on
   the control stream, so support is known before any publish. No new
   handshake.
2. **Topic streams** — a peer opens a *bidirectional* stream per topic it wants
   to publish on, but treats it as **unidirectional**. The responder MUST NOT
   write; if it does, the initiator SHOULD close the connection. Protocol id:
   **`/gsts/v0beta`**.
3. **Control stream** — the original gossipsub stream becomes the *control
   stream*. Application messages (`Message`, `PartialMessagesExtension`) MUST
   NOT be sent on it once this extension is active.
4. **Header** — the initiator first sends one length-prefixed `TopicRPCHeader`
   (carrying the topic), then only length-prefixed `TopicRPC` messages.
5. **Limits / scoring** — receiver SHOULD allow ≤ 3 concurrent streams per
   topic per peer and downscore peers that exceed it; initiator SHOULD use 1
   per topic. Receiver SHOULD process multiple streams for a topic in receive
   order. Receiving a topic stream for a non-subscribed topic SHOULD be
   downscored, **except** for recently-unsubscribed topics (unsubscribe race).
6. **Lifecycle** — a topic stream is created when a node first publishes to a
   peer on a topic; closed when the peer unsubscribes or the publisher stops
   publishing. Either side may close. Implementations MAY keep streams open
   only for mesh/fanout peers and use short-lived streams for `IWANT`
   responses.
7. **Topic-scoped messages** — publish a `TopicScopedMessage` with the topic
   name **omitted** on the wire. On receipt, reconstruct the full `Message` by
   setting `topic` from the stream's `TopicRPCHeader` before verifying the
   signature, and SHOULD populate `topic` before delivering to the
   application.
8. **Partial messages** — if the partial-messages extension is also negotiated,
   `PartialMessagesExtension` messages MUST be sent on the topic stream (inside
   `TopicRPC.partial`) with `topicID` omitted on the wire and repopulated after
   reading.

### Protobuf additions (from `extensions/extensions.proto`)

```proto
message ControlExtensions {
  optional bool partialMessages = 10;
  optional bool testExtension   = 6492434;
  optional bool topicStreams    = 6492435; // experimental field number
}

message TopicRPCHeader { optional string topic = 1; }

// Identical to Message, except field 4 (topic) is unused on the wire.
message TopicScopedMessage {
  optional string from             = 1;
  optional bytes  data             = 2;
  optional bytes  seqno            = 3;
  optional string unset_topic_name = 4; // for signature computation only
  optional bytes  signature        = 5;
  optional bytes  key              = 6;
}

message TopicRPC {
  optional TopicScopedMessage      publish = 1;
  optional PartialMessagesExtension partial = 2;
}
```

> Note the field-number parity between `TopicScopedMessage` and `Message`
> (`from/data/seqno/_/signature/key` = `1/2/3/4/5/6`). Because the signature is
> computed over the marshalled `Message` (see `verifyMessageSignature` in
> `sign.go`), reconstruction is a straight field copy plus setting `topic`
> (field 4) from the header. This is the crux of the wire design.

---

## 2. Relevant current architecture

References are to files at the repo root unless noted.

### One stream per direction, one queue per peer

- **Outbound:** `PubSub.handleNewPeer` (`comm.go`) opens a single stream with
  `host.NewStream(ctx, pid, rt.Protocols()...)` and runs
  `handleSendingMessages`, which drains a single per-peer `*rpcQueue` stored in
  `PubSub.peers[pid]`. The first frame is the "hello packet"
  (`getHelloPacket` → subscriptions + extensions via
  `OnNewOutboundStream`).
- **Inbound:** `PubSub.handleNewStream` (`comm.go`) is registered for every
  protocol id in `rt.Protocols()` (`pubsub.go` constructor, ~L622-628). It
  reads varint-delimited `RPC` protobufs and pushes
  `incomingUnion{kind: incomingKindRPC}` onto `PubSub.incoming`. It also emits
  `incomingKindNewStream` / `incomingKindClosedStream`.
- **Dispatch:** `PubSub.processLoop` (`pubsub.go`) is the single-threaded
  owner of router state. It routes `incomingKindRPC` → `handleIncomingRPC`,
  `…NewStream` → `rt.OnNewIncomingStream`, `…ClosedStream` →
  `onClosedIncomingStream` + `rt.OnClosedIncomingStream`.
- **Send:** `GossipSubRouter.sendRPC` (`gossipsub.go`, ~L1536) piggybacks
  pending control/gossip and pushes onto `PubSub.peers[p]` (the single queue).
- **Publish:** `GossipSubRouter.Publish` → `rpcs(msg)` (`gossipsub.go`, ~L1352)
  builds `rpcWithMessages(msg.Message)` and calls `sendRPC` for each target
  peer (mesh/fanout/flood/direct), already skipping peers that requested
  partial messages.

### Extensions framework (the template to follow)

- `extensions.go` defines `PeerExtensions{TestExtension, PartialMessages}`,
  `extensionsState` (tracks `myExtensions`, `peerExtensions`,
  `sentExtensions`), and the `ExtendRPC` / `peerExtensionsFromRPC` plumbing.
- An extension is "negotiated" for a peer once we have **both sent and
  received** the extensions control message — see
  `extensionsOnNewOutboundStream` / `extensionsOnClosedOutboundStream`.
- `WithTestExtension` and `WithPartialMessagesExtension` (`extensions.go`) show
  the option-wiring pattern; `testExtension` (`testextension.go`) is the
  minimal reference extension.
- Extensions require gossipsub v1.3: `GossipSubFeatureExtensions` is only true
  for `GossipSubID_v13` (`gossipsub_feat.go`).

### Message ingestion, signing, IDs

- `verifyMessageSignature` (`sign.go`) marshals the full `Message` (minus
  `signature`/`key`) — **topic included** — so the topic MUST be present before
  verification.
- `msgIDGenerator.ID` (`midgen.go`) selects the id function by
  `msg.GetTopic()`, so topic MUST be set before `pushMsg`.
- `handleIncomingRPC` (`pubsub.go`, ~L1397) processes subscriptions, vets the
  peer via `AcceptFrom`, then for each `Publish` builds a `Message` and calls
  `pushMsg`. Topic-stream messages can reuse this entire path simply by
  reconstructing a `Message` (with topic) and feeding an `RPC` onto
  `PubSub.incoming`.
- Partial routing currently flows through `partialMessageRouter.SendRPC`
  (`extensions.go`), which wraps `RPC{Partial: …}` and calls `sendRPC` on the
  control stream — this is exactly what must be redirected to topic streams.

---

## 3. Gap analysis — the hard parts

1. **No per-topic outbound stream concept exists.** The whole send path assumes
   one queue (`PubSub.peers[pid]`). We must introduce a per-`(peer, topic)`
   outbound stream + writer goroutine, owned/registered alongside the existing
   single stream, and route topic-scoped publishes to it.
2. **A second inbound protocol handler is needed.** `/gsts/v0beta` is *not* in
   `rt.Protocols()` and must not be (that list drives the control-stream dial
   and feature tests). It needs its own `SetStreamHandler` + read loop with a
   header-then-body state machine.
3. **Concurrency model.** Router state (`mesh`, `fanout`, `peers`,
   `extensions`) is single-threaded via `processLoop`/`eval`. Per-stream writer
   goroutines must not touch that state directly; topic-stream bookkeeping must
   be self-contained and independently locked.
4. **Keep the router transport-agnostic.** Topic streams are a *transport*
   optimization, not a routing decision. The router (`gossipsub.go`) should keep
   producing normal `RPC`s and calling `sendRPC` exactly as today; the
   `comm.go` send layer should transparently peel topic-scoped content
   (`Publish` messages, `Partial`) onto the right `/gsts/v0beta` stream and leave
   control traffic (subs, IHAVE/IWANT/GRAFT/PRUNE/IDONTWANT, extensions) on the
   control stream. This keeps mesh/fanout/scoring logic untouched and confines
   the change to the wire layer. (See §4.3.)
5. **Inbound ordering (kept in `comm.go`).** Topic messages are delivered as
   ordinary `incomingKindRPC`, so `pubsub.go`'s `processLoop` /
   `handleIncomingRPC` machinery does not change. Correctness reduces to
   **ordering**: the peer's control-stream hello (which MUST carry the extensions
   control message) must be enqueued before any topic message, else
   `extensions.HandleRPC` would mistake a topic RPC for the "first RPC = hello".
   `comm.go` enforces this by buffering at most one topic frame until the hello
   is enqueued, and by dropping topic messages once the control stream is closed.
   A topic-stream close must **not** be treated as the control stream closing.
   (See §4.4 and §4.5.)
6. **Partial-messages interplay.** When both extensions are on, partial RPCs
   ride the topic stream. With the agnostic design this falls out for free — the
   send layer routes `RPC.Partial` the same way it routes `Publish`.
7. **Lifecycle + scoring.** Open/close rules, the ≤3-streams-per-topic limit,
   downscoring of unsubscribed-topic streams (with an unsubscribe grace
   window), and the responder-must-not-write rule.

---

## 4. Proposed design

### 4.1 Protobuf (`pb/rpc.proto` + regenerate)

Add `topicStreams` to `ControlExtensions`, and the `TopicRPCHeader`,
`TopicScopedMessage`, and `TopicRPC` messages exactly as in the spec's
`extensions.proto`. Regenerate `pb/rpc.pb.go` via `pb/Makefile`
(`protoc --go_out`). Keep `pb/rpc.proto` aligned with the upstream
`extensions.proto` registry.

Helpers (new, e.g. `topicstreams.go`):

- `messageToTopicScoped(*pb.Message) *pb.TopicScopedMessage` — copies
  `from/data/seqno/signature/key`, leaves topic unset.
- `topicScopedToMessage(*pb.TopicScopedMessage, topic string) *pb.Message` —
  copies fields back and sets `Topic = topic`.

### 4.2 Negotiation

- Add `TopicStreams bool` to `PeerExtensions`; wire into
  `peerExtensionsFromRPC` and `ExtendRPC` (`extensions.go`).
- Add `WithTopicStreams(...)` option setting
  `myExtensions.TopicStreams = true` (mirroring `WithTestExtension`); it enables
  the transport-layer behavior in §4.3 rather than installing anything on the
  router.
- Gate everything on `GossipSubFeatureExtensions` (v1.3) like the other
  extensions.
- Drive activation/teardown from `extensionsOnNewOutboundStream` /
  `extensionsOnClosedOutboundStream`: when both sides advertise `topicStreams`,
  flip the peer's `peerTransport.topicStreamsEnabled` flag (§4.3); on close,
  tear down all per-topic streams to that peer.

### 4.3 Outbound: transparent multiplexing in `comm.go` (router stays agnostic)

**The router does not change.** `Publish` / `rpcs` / `sendRPC` keep building
normal `RPC`s and enqueueing them onto the peer's single `*rpcQueue`
(`PubSub.peers[pid]`) exactly as today. Topic-stream routing is pushed entirely
into the per-peer send path in `comm.go`, so gossipsub's mesh/fanout/scoring
logic never learns that topic streams exist.

Today the per-peer writer is `handleSendingMessages` (`comm.go`), which drains
the queue and writes each `RPC` to the one control stream. We turn it into a
small per-peer **stream set** that owns:

- the control stream (as today), and
- a lazily-created `map[string]*outboundTopicStream` keyed by topic, each a
  `/gsts/v0beta` stream with a header-first writer.

For every dequeued `RPC` the writer decides, per piece, where bytes go:

- **Topic streams disabled for this peer** (peer is pre-v1.3, or the
  `topicStreams` extension was not mutually negotiated): write the whole `RPC`
  on the control stream — current behavior, zero change.
- **Topic streams enabled:**
  - For each `msg` in `rpc.Publish`: open/lookup the stream for `msg.Topic`
    (sending `TopicRPCHeader{topic}` first), then write
    `TopicRPC{publish: messageToTopicScoped(msg)}` (topic omitted on the wire).
  - If `rpc.Partial` is set: route it to the stream for `rpc.Partial.TopicID` as
    `TopicRPC{partial: …}` with `topicID` cleared on the wire.
  - Write whatever remains (`Subscriptions`, `Control`) on the control stream.

This means the *same* `RPC{Publish:…}` the router produces today is split by the
writer at send time; there is no new router API and no `SendMessage`/
`SendPartial` call sites in `gossipsub.go`.

**How the writer learns negotiation state.** Negotiation completes on the
`processLoop`/`eval` thread inside `extensionsState` (both sides advertised
`topicStreams`). We give each peer a small shared `peerTransport` struct created
when the peer is added; it holds an atomic `topicStreamsEnabled` flag (plus the
host handle needed to open streams). `extensionsOnNewOutboundStream` flips the
flag once; the writer reads it per `RPC`. The flag is written exactly once by
the eval thread and only read by the writer, so an atomic bool is sufficient and
race-free. Until it flips, everything stays on the control stream — which is
correct, because the spec guarantees the extensions message precedes any publish
(§Negotiation), so no topic message can be enqueued before negotiation is known.

**Responder-must-not-write guard.** Each outbound topic stream also gets a tiny
reader goroutine: any byte read from it is a protocol violation, so we log and
close the connection (analogous to `handlePeerDead` in `comm.go`).

**Concurrency.** The `peerTransport` and its topic-stream map are owned by the
peer's writer goroutine (plus the one atomic flag set by eval). The writer never
touches router maps; the router never touches transport maps. No new shared
mutable router state is introduced.

Helpers `messageToTopicScoped` / `topicScopedToMessage` live in the transport
layer (§4.1).

### 4.4 Inbound: `/gsts/v0beta` handler (in `comm.go`)

A dedicated reader, living in `comm.go` (registered as a stream handler for
`/gsts/v0beta`, separate from `handleNewStream`), handles each inbound topic
stream:

1. Read the first frame as `TopicRPCHeader`; extract `topic`.
2. Loop reading `TopicRPC` frames; reconstruct an **ordinary** `RPC`:
   - `publish` → `topicScopedToMessage(ts, topic)` → `RPC{Publish: [m], from: peer}`
     (full `Message` with topic, so the signature verifies, per the spec);
   - `partial` → set `partial.TopicID = topic` → `RPC{Partial: partial, from: peer}`.
3. Push it onto `PubSub.incoming` as an ordinary **`incomingKindRPC`** — the same
   kind the control-stream reader already uses — subject to the ordering rule in
   §4.5.

Because topic messages arrive as a normal `incomingKindRPC`, the `processLoop` /
`incomingUnion` / `handleIncomingRPC` machinery in `pubsub.go` does **not**
change. `handleIncomingRPC` validates and forwards the publish exactly as for a
control-stream message, and a reconstructed `RPC{Partial}` routes through the
existing `rt.HandleRPC → extensions.HandleRPC → partialMessagesExtension` path.
No new incoming kind and no separate ingestion path are introduced.

### 4.5 Inbound ordering: the hello comes first — all in `comm.go`

> Addresses: the extensions control message MUST be applied before any topic
> message, and topic messages must be dropped once the control stream is closed.
> All of this is enforced in `comm.go`; `pubsub.go` does not change.

**Why ordering is the whole game.** Delivering topic messages as ordinary
`incomingKindRPC` is correct **iff** the peer's control-stream hello is processed
before any of its topic messages. The hello MUST carry the extensions control
message (gossipsub v1.3); if a reconstructed topic RPC reached `handleIncomingRPC`
first, `rt.HandleRPC → extensions.HandleRPC` would hit the "first RPC from this
peer ⇒ extensions hello" branch (`extensions.go`) and corrupt
`peerExtensions[peer]`. Conversely, once the hello has been processed,
`peerExtensions[peer]` is set, the misfire is impossible, the reconstructed RPC
carries no subscriptions/extensions (so those code paths are no-ops), and a
`Partial` routes correctly through the existing extensions handler.

`PubSub.incoming` is FIFO and single-consumer, so **enqueue order = process
order**. It is therefore enough for `comm.go` to *enqueue* the hello before any
topic message — no `processLoop` changes, no extra incoming kind, no buffer in
`pubsub.go`.

**Coordination (in `comm.go`).** A small per-peer object (e.g.
`helloEnqueued chan struct{}` plus a `closed` flag) ties the two inbound readers
together:

- The control-stream reader (`handleNewStream`) closes `helloEnqueued`
  immediately after it enqueues the peer's first control RPC (the hello).
- The topic-stream reader, after reading its first `TopicRPC`, **buffers that one
  frame** and `select`s on `helloEnqueued` / `closed` / `ctx` before enqueuing.
  At most one frame is buffered, because the reader blocks before reading the
  next. Once the hello is enqueued it pushes the buffered frame, then streams the
  rest normally.

So the three cases are handled entirely in `comm.go`:

- **hello not yet enqueued** ⇒ hold one frame, deliver it right after the hello;
- **control stream closed / torn down** ⇒ drop the buffered frame, reset the
  topic stream, and stop reading. The control reader marks `closed` before it
  enqueues its `ClosedStream`, and the topic reader checks `closed` before each
  enqueue, so `processLoop` never sees a topic RPC after the control
  `ClosedStream` — i.e. "drop topic messages once the control stream is closed",
  enforced in `comm.go`;
- **hello applied, stream open** ⇒ enqueue normally.

**Topic-stream lifecycle never uses the control-stream notifications.** The
`/gsts/v0beta` reader must **not** emit `incomingKindNewStream` /
`incomingKindClosedStream` — those drive `onClosedIncomingStream` →
`clearPeerFromTopicsState` (`pubsub.go`), which would wipe the peer's
subscriptions and `peerExtensions`. A topic stream closing therefore never
disturbs control-stream state, and vice versa. (If the *connection* drops, all
its streams fail together; the existing `handleDeadPeers` teardown plus the
transport layer's per-stream cleanup both run.)

**Limits, scoring, ordering** (in the transport layer / via a score callback):

- Track concurrent inbound streams per `(peer, topic)`; if > 3, downscore
  (`reportMisbehavior`) and reset the offending stream.
- If `topic` is not in our subscriptions **and** not recently unsubscribed,
  downscore. This is implemented as a **general PubSub-level rule**, not a
  topic-streams-only one: today `handleIncomingRPC` (`pubsub.go`) *silently
  ignores* a publish for an unsubscribed topic; we factor a shared helper
  (backed by a `recentlyUnsubscribed` TTL set recorded in
  `handleRemoveSubscription`) and apply it to both the control-stream publish
  path and the topic-stream path. The TTL set honors the unsubscribe-race grace
  the spec requires. (Behavior change for the normal path — silently-ignore →
  downscore — so keep it conservative and consistent with `score.go`.)
- A single reader goroutine per stream preserves per-stream receive order; the
  spec's "process multiple streams for a topic in receive order" is an explicit
  open item (see §7) since the shared `incoming` channel only loosely orders
  across streams.

### 4.6 Partial messages integration

With the agnostic send path (§4.3), no extra wiring is needed:
`partialMessageRouter.SendRPC` (`extensions.go`) keeps doing
`gs.sendRPC(RPC{Partial: …})`, and the `comm.go` writer routes that `Partial`
onto the topic stream (clearing `topicID` on the wire) just like a `Publish`.
Inbound, the §4.4 handler repopulates `Partial.TopicID` from the header and
pushes an ordinary `RPC{Partial}`, which the unchanged
`handleIncomingRPC → rt.HandleRPC → extensions.HandleRPC` path hands to
`partialMessagesExtension.HandleRPC`.

### 4.7 Lifecycle, limits, scoring

All outbound topic-stream lifecycle lives in the transport layer (§4.3); the
router only emits intents the writer already sees.

- **Open** lazily inside the writer on the first `Publish`/`Partial` for a
  `(peer, topic)`.
- **Close** when: the peer unsubscribes from the topic, when we leave/stop
  publishing the topic, or when the peer leaves the mesh/fanout (optional
  optimization: keep open only for mesh/fanout, short-lived for `IWANT`). The
  unsubscribe/leave signals can be delivered to the writer via the same
  per-peer `peerTransport` (e.g. a small command channel) so the router need
  only continue calling `Leave`/processing unsubscribes as today.
- **Teardown** all topic streams to a peer (inbound and outbound) when the
  control stream closes (`onClosedIncomingStream` /
  `extensionsOnClosedOutboundStream`), when the connection drops
  (`handleDeadPeers`), and on blacklist. After the control stream closes, any
  further inbound topic messages are dropped by the §4.5 control-stream check
  (enforced in `comm.go`).
- **Scoring** reuses `reportMisbehavior` / the score subsystem for: responder
  writing on a topic stream, > 3 concurrent inbound streams per topic, and
  unsubscribed-topic publishes (outside the grace window — via the shared
  general rule above, applied to both control-stream and topic-stream publishes).
- **Resource limits**: ensure topic streams respect libp2p resource-manager
  scopes; cap total outbound topic streams per peer.

### 4.8 Public API

- `WithTopicStreams()` `Option` (no required callbacks; optional config struct
  for limits/short-lived behavior, following `TestExtensionConfig`).
- No new `GossipSubFeature` constant is required (negotiation is via the
  extensions control message), but topic streams are only active on v1.3.

---

## 5. Implementation phases

1. **Proto + helpers.** Update `pb/rpc.proto`, regenerate `pb/rpc.pb.go`, add
   `messageToTopicScoped` / `topicScopedToMessage` + round-trip unit tests
   (incl. signature verification parity with `Message`).
2. **Negotiation plumbing.** `PeerExtensions.TopicStreams`, `ExtendRPC`,
   `peerExtensionsFromRPC`, `WithTopicStreams`, activation hooks. Unit tests
   mirroring the extension/feature tests.
3. **Outbound transparent multiplexing (`comm.go`).** Per-peer stream set,
   lazy `/gsts/v0beta` open + header, writer splits `Publish`/`Partial` onto
   topic streams vs control traffic on the control stream, responder-write
   guard, `peerTransport` + atomic negotiation flag set from
   `extensionsOnNewOutboundStream`. **No changes to `gossipsub.go` routing.**
4. **Inbound `/gsts/v0beta` reader (in `comm.go`).** Header state machine,
   message/partial reconstruction, push as ordinary `incomingKindRPC`; enforce
   hello-before-topic ordering and control-stream-closed dropping in `comm.go`
   (per-peer `helloEnqueued`/`closed`, buffer at most one). No `pubsub.go`
   machinery change; per-topic limits + downscoring handled in the transport
   layer.
5. **Verify the split.** Confirm the control stream no longer carries `Message`
   for negotiated peers and that all existing routing tests still pass
   unchanged (since the router is untouched).
6. **Lifecycle polish + scoring.** Mesh/fanout-only retention, short-lived
   `IWANT` streams, score penalties, resource caps.
7. **Tests + docs.** Integration tests, backwards-compat, and a short
   `README`/doc note. Optionally update `extensions/extensions.proto` registry
   upstream.

---

## 6. Testing plan

- **Unit:** proto round-trip; `messageToTopicScoped`/`topicScopedToMessage`
  signature verification parity; negotiation flag plumbing.
- **Negotiation:** two routers with `WithTopicStreams` exchange
  `ControlExtensions.topicStreams`; assert activation only when both advertise.
  (Model on `gossipsub_feat_test.go` and existing extension tests.)
- **End-to-end delivery:** two gossipsub nodes (v1.3 + `WithTopicStreams`)
  subscribe to a topic and publish; assert the message is delivered **and** that
  it traveled on a `/gsts/v0beta` stream, not the control stream (inspect via
  the tracer / stream protocol id). Reuse the stream-level skeleton harnesses
  in `gossipsub_test.go` (`skeletonGossipsub`, ~L4482) and
  `gossipsub_peer_lifecycle_test.go`.
- **HOL / multi-topic:** publish on several topics concurrently; confirm
  independent streams.
- **Backwards compatibility:** topic-streams node ↔ non-topic-streams node
  falls back to control-stream `Message`s; no regression in existing tests.
- **Partial + topic streams:** combined negotiation routes partials on the
  topic stream with `topicID` omitted/restored.
- **Control-stream gating:** after the control stream closes, topic-stream
  messages from that peer are dropped (not delivered to subscribers) and the
  peer's topic streams are torn down; messages on a topic stream while the
  control stream is open are delivered normally.
- **Hello-ordering race:** a topic frame that arrives just before the control
  hello is buffered (at most one) and delivered after the hello is applied; a
  second early frame is dropped; the extensions control message is always
  applied first.
- **Lifecycle/misbehavior:** responder writes ⇒ connection closed; > 3 streams
  per topic ⇒ downscore; unsubscribed-topic stream ⇒ downscore (and *not*
  within the unsubscribe grace window); stream closed on unsubscribe/leave.

---

## 7. Risks & open questions

- **Biggest risk:** introducing per-`(peer, topic)` streams into a send path
  that assumes one stream/queue per peer (`PubSub.peers`). Keeping the router
  agnostic (§4.3) contains the change to `comm.go`, but the writer now owns
  multiple streams and a negotiation flag shared with the eval thread — that
  hand-off must stay simple (single atomic, set once).
- **Cross-stream ordering / synchronization:** the single `incoming` channel
  prevents data races but gives no ordering between the control stream and topic
  streams (e.g. a control GRAFT vs a topic publish may reorder). This is
  acceptable per the spec's intent (decoupling topics) but must be validated;
  the "process multiple streams per topic in receive order" SHOULD is an open
  item that may need per-`(peer, topic)` sequencing.
- **Resource usage:** many topics ⇒ many streams; need caps and resource-
  manager awareness; revisit `peerOutboundQueueSize` semantics per topic.
- **`/gsts/v0beta` is experimental/beta** — protocol id and field number
  (`6492435`) are not final; keep them isolated/configurable.
- **Interaction with message batching** (`messagebatch.go`) and IDONTWANT — map
  out how batched/partial flows interact with topic streams.
- **Spec maturity:** Working Draft (r0). Track upstream changes on
  `marco/topic-message-streams` before stabilizing.

---

## 8. Environment notes

The build/codegen toolchain has been provisioned on this VM and verified
(`go build ./...` and `make -C pb clean && make -C pb` both succeed, with the
regenerated `pb/*.pb.go` byte-identical to what is committed):

- **Go 1.25.11** at `/usr/local/go` (the module requires Go ≥ 1.25; the image's
  default Go 1.22.2 could not auto-download the toolchain).
- **protoc 34.1** — its reported compiler version (`v7.34.1`) matches the header
  in the committed generated files.
- **protoc-gen-go v1.36.6** — matches the committed generator version.

These installs live only in the current VM; to make them permanent for future
Cloud Agents, update the cloud-agent environment config (e.g. via an env-setup
agent) to install the same versions and put them on `PATH`.
