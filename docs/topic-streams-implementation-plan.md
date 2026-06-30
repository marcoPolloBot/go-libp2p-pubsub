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
   goroutines must not touch that state directly; the topic-stream manager's
   bookkeeping must either live behind `eval` or be a self-contained,
   independently-locked component fed by the router.
4. **Routing split.** `rpcs`/`Publish` must send full messages to the topic
   stream for negotiated peers and keep using the control stream for everyone
   else. Control traffic (subs, IHAVE/IWANT/GRAFT/PRUNE/IDONTWANT, extensions)
   always stays on the control stream.
5. **Partial-messages interplay.** When both extensions are on, partial RPCs
   move to the topic stream. The combination must be explicitly handled (the
   spec calls this out).
6. **Lifecycle + scoring.** Open/close rules, the ≤3-streams-per-topic limit,
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
  `myExtensions.TopicStreams = true` and installing the topic-stream manager on
  the router (mirroring `WithPartialMessagesExtension`).
- Gate everything on `GossipSubFeatureExtensions` (v1.3) like the other
  extensions.
- Drive activation/teardown from `extensionsOnNewOutboundStream` /
  `extensionsOnClosedOutboundStream`: when both sides advertise `topicStreams`,
  mark the peer topic-stream-capable; on close, tear down all per-topic streams
  to that peer.

### 4.3 Outbound: `topicStreamManager`

A new component (initialized in `Attach`, given the `host`, the router, and a
`sendonTopicStream` entry point) that maintains:

```
map[peer.ID]map[string]*outboundTopicStream   // (peer, topic) -> stream
```

Per stream: a `network.Stream` (`/gsts/v0beta`), a bounded queue, and a writer
goroutine (modeled on `handleSendingMessages` in `comm.go`) that:

1. writes the length-prefixed `TopicRPCHeader{topic}` first, then
2. writes length-prefixed `TopicRPC` frames.

API used by the router:

- `SendMessage(p, topic, *pb.Message)` — lazily opens the stream (header on
  first send), enqueues `TopicRPC{publish: messageToTopicScoped(msg)}`.
- `SendPartial(p, topic, *pb.PartialMessagesExtension)` — enqueues
  `TopicRPC{partial: …}` with `topicID` cleared on the wire.
- `CloseTopic(p, topic)` / `CloseAll(p)` — lifecycle.
- A short-lived variant for `IWANT` responses (open, send, close) per the
  spec's MAY.

The initiator also runs a tiny reader on each outbound stream to enforce
"responder MUST NOT write": any byte read ⇒ log + close the connection (treat
as protocol violation), analogous to `handlePeerDead` (`comm.go`).

Concurrency: the manager owns its own mutex; it never touches router maps
directly. The router calls into it from `processLoop`/`eval` (single-threaded),
and writer goroutines only touch their own stream + queue.

### 4.4 Inbound: `/gsts/v0beta` handler

Register a dedicated handler in the constructor (`pubsub.go`) — separate from
`handleNewStream`. The handler:

1. Reads the first frame as `TopicRPCHeader`; extracts `topic`.
2. Loops reading `TopicRPC` frames. For each:
   - `publish` → `topicScopedToMessage(ts, topic)`, wrap into
     `RPC{Publish: []*Message{m}, from: peer}`, push
     `incomingUnion{kind: incomingKindRPC}` onto `PubSub.incoming`. This reuses
     the existing validation/forwarding pipeline unchanged
     (`handleIncomingRPC` → `pushMsg` → signature verify with topic present).
   - `partial` → set `partial.TopicID = topic`, route to the partial extension
     (via the same `incoming`/`HandleRPC` path used today, wrapping
     `RPC{Partial: …}`).
3. Enforces limits via `eval`/router callbacks:
   - Track concurrent inbound streams per `(peer, topic)`; if > 3, downscore
     (`reportMisbehavior`) and reset the stream.
   - If `topic` is not in our subscriptions **and** not recently unsubscribed,
     downscore. Maintain a small recently-unsubscribed LRU/TTL set keyed by
     topic to honor the unsubscribe-race grace.
   - Process frames in receive order (single reader goroutine per stream;
     pushing onto the ordered `incoming` channel preserves ordering across
     streams reasonably).

Stream close handling mirrors `handleNewStream`'s `incomingKindClosedStream`
bookkeeping so per-peer/per-topic inbound counters are cleaned up.

### 4.5 Routing changes

In `GossipSubRouter.rpcs` / `Publish` (`gossipsub.go`):

- For each target peer `p` and the message's `topic`, if topic streams are
  negotiated with `p` (`extensions.peerExtensions[p].TopicStreams &&
  myExtensions.TopicStreams`), call `topicStreamManager.SendMessage(p, topic,
  msg)` **instead of** `sendRPC` (control stream).
- All other peers keep the existing `sendRPC` path.
- Control messages and subscriptions are untouched — they always use the
  control stream.
- `IDONTWANT` semantics are preserved: IDONTWANT stays on the control stream;
  the `unwanted` checks in `rpcs` continue to gate which messages are sent
  (whether via control or topic stream).

### 4.6 Partial messages integration

`partialMessageRouter.SendRPC` (`extensions.go`) becomes topic-aware: when topic
streams are negotiated with the target peer, route the
`PartialMessagesExtension` through `topicStreamManager.SendPartial(p, topicID,
…)` (clearing `topicID` on the wire) rather than `gs.sendRPC(RPC{Partial})`.
Inbound partials arriving on a topic stream get `topicID` repopulated from the
header before being handed to `partialMessagesExtension.HandleRPC`.

### 4.7 Lifecycle, limits, scoring

- **Open** lazily on first publish to a peer for a topic.
- **Close** when: we observe the peer's unsubscribe for that topic (already
  tracked in `handleIncomingRPC` via `p.topics`), when we leave/stop publishing
  the topic (`Leave`), or when the peer leaves the mesh/fanout (optional
  optimization: keep open only for mesh/fanout, short-lived for `IWANT`).
- **Teardown** all topic streams to a peer on control-stream close
  (`extensionsOnClosedOutboundStream`) and on blacklist.
- **Scoring** reuses `reportMisbehavior` / the score subsystem for: responder
  writing on a topic stream, > 3 concurrent inbound streams per topic, and
  unsubscribed-topic streams (outside the grace window).
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
3. **Outbound `topicStreamManager`.** Stream open + header + writer goroutine +
   responder-write guard + lifecycle/close. Owned-state concurrency.
4. **Inbound `/gsts/v0beta` handler.** Header state machine, message
   reconstruction, push onto `incoming`, per-topic limits + downscoring +
   unsubscribe grace.
5. **Routing split.** `rpcs`/`Publish` send via topic streams for negotiated
   peers; verify control stream no longer carries `Message`.
6. **Partial-messages integration.** Topic-aware `SendRPC`; inbound `topicID`
   repopulation.
7. **Lifecycle polish + scoring.** Mesh/fanout-only retention, short-lived
   `IWANT` streams, score penalties, resource caps.
8. **Tests + docs.** Integration tests, backwards-compat, and a short
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
- **Lifecycle/misbehavior:** responder writes ⇒ connection closed; > 3 streams
  per topic ⇒ downscore; unsubscribed-topic stream ⇒ downscore (and *not*
  within the unsubscribe grace window); stream closed on unsubscribe/leave.

---

## 7. Risks & open questions

- **Biggest risk:** the per-`(peer, topic)` outbound stream model is a
  structural change to a codebase that assumes one queue per peer
  (`PubSub.peers`). Concurrency between writer goroutines and the
  single-threaded `processLoop`/`eval` must be carefully bounded.
- **Resource usage:** many topics ⇒ many streams; need caps and resource-
  manager awareness; revisit `peerOutboundQueueSize` semantics per topic.
- **Ordering:** spec says receivers SHOULD process multiple streams for a topic
  in receive order — confirm the single `incoming` channel preserves adequate
  ordering, or add per-topic sequencing.
- **`/gsts/v0beta` is experimental/beta** — protocol id and field number
  (`6492435`) are not final; keep them isolated/configurable.
- **Interaction with message batching** (`messagebatch.go`) and IDONTWANT — map
  out how batched/partial flows interact with topic streams.
- **Spec maturity:** Working Draft (r0). Track upstream changes on
  `marco/topic-message-streams` before stabilizing.

---

## 8. Environment notes

- The module requires **Go 1.25** (`go.mod`), but this VM currently has Go
  1.22.2 and the 1.25 toolchain auto-download fails ("toolchain not
  available"). Building/testing the implementation requires provisioning Go
  1.25.
- Regenerating `pb/rpc.pb.go` requires `protoc` + `protoc-gen-go`, which are
  not installed here. These should be added to the cloud-agent environment
  before implementation begins.
