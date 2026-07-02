# RFC 0012: LSN-based read consistency — surface the Lance commit version as the read-your-writes token

Tracking issue: _TBD_

> **Status:** draft, proposal. This RFC proposes a direction rather than keeping
> the options open: replace the timestamp-shaped consistency watermark with an
> **LSN (log sequence number)** drawn from the commit version the engine already
> tracks per namespace, and expose it as a read-your-writes token on the read
> path (writes return an LSN; reads may require one). The model deliberately
> mirrors [TopK's LSN-based consistency](https://docs.topk.io/concepts#lsn-based-consistency)
> (`upsert() → lsn`; `query(lsn=…, consistency=…)`).
>
> The engine owns the **consistency primitive** — commit ordering, the durable
> LSN, and "wait until the read path has applied LSN N" (engine). Layer owns
> **carrying the token on the inbound wire** — how a Turbopuffer-shaped request
> and response surface the LSN, and the per-request consistency policy (edge).
> The edge twin is `../../../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md`;
> this RFC is the engine half.

## Summary

hev search's consistency story today is a **heuristic timestamp watermark** owned
by Layer, and on `kind: search` its per-row half is **inert**. The engine already
computes the right primitive — a monotonic, durable **commit version** per
namespace (`table_version`, `result.rs:208`) — but never exposes it on the read
path. This RFC surfaces that version as an LSN:

- **Writes return the LSN.** `/upsert` and the terminal state of an `/import`
  operation report the `table_version` produced by their commit.
- **Reads may require an LSN.** `/query` (and `/list`) accept an `lsn` plus a
  `consistency` level (`indexed` / `balanced` / `strong`). A `strong` read blocks
  until the read path has applied a version `>= lsn`, or is rejected for the
  client to retry. `balanced` is the default; `indexed` is the fast eventual read.

The primitive is already there and the read cache-aside already keys on it
(`service_cache_aside.rs:85`, `object_cache.rs:24`). This is mostly *exposure and
read-path gating*, not new ordering machinery.

## Motivation: the timestamp watermark was Turbopuffer-shaped, and it is inert here

The consistency watermark was designed against Turbopuffer's write pattern — bulk
upserts of ~10k rows. When writes land in discrete batches, "stable as of time T"
is coherent: a batch has a wall-clock, and a read filtered to `<= T` is a
defensible freshness bound. So Layer stamps `_hevlayer_upserted_at` (epoch-ms) on
every write and, while a namespace's index is `Updating`, injects an
`_hevlayer_upserted_at <= watermark` predicate on the query path
(`layer-gateway routes/query.rs:70,153,246`).

Two problems, one fatal:

1. **The predicate is inert on `kind: search` — it fails open.** The engine strips
   every `_hevlayer_*` attribute before writing a row
   (`layer-gateway clients/search.rs:355–361`, filter
   `key != "text" && !key.starts_with("_hevlayer_")`). The stamp never reaches an
   engine column, so the injected predicate matches nothing and does nothing.
   Injection is gated on index-updating status, **not** store kind
   (`routes/query.rs:153`), so a `kind: search` namespace mid-update silently
   serves potentially-stale reads with **no error raised**. Stable reads are a
   no-op that looks like a feature. (This was going to be filed as a bug; it is
   folded here as the motivating defect.)

2. **A timestamp cannot give exact read-your-writes, even where it is honored.**
   The engine *does* keep its own write-stamp — `_ingested_at`, a microsecond
   column used for `/list` and `/scan` cursors and the default scalar index. But
   within a batch every row shares one `_ingested_at`
   (`api_scalar_index.rs:146`), and wall-clocks are not monotonic across writers
   or immune to skew. So even reusing `_ingested_at` as the watermark could not
   express "read my specific write" — only "read roughly around this time." The
   store capability matrix already concedes this, marking the row **heuristic**
   and stating the engine has **no LSN**
   (`layer/site/src/content/docs/kubernetes/store-support.mdx:43`).

Meanwhile the engine is **commit/log-structured**, not batch-timestamp-structured.
LanceDB advances a per-namespace `table_version` on every commit — tests assert it
advances on write and that import moves it `before + 1`
(`manager_import.rs:223–227`, `manager_namespace_info.rs:109,122`), the manifests
live at versioned object paths (`_versions/N.manifest`, `_transactions/`,
`_latest`; `object_cache.rs:24,997`), and the read cache-aside already invalidates
by it (`service_cache_aside.rs:85`). The engine's own truth is a monotonic
sequence number. Stamping a Layer-side timestamp into user rows — which the engine
then strips — is the impedance mismatch that produced the inert predicate. An LSN
cannot be stripped, because it is commit metadata, not a row attribute.

## Proposal

### Writes return an LSN

Every committing write reports the `table_version` its commit produced:

- **`/upsert`** — synchronous; return the post-commit version, e.g. `{"lsn": 42,
  "billing": …}`.
- **`/import`** — async (`202` + `/operations/{id}`). The operation's terminal
  status carries the LSN of the commit it landed. This answers RFC 0008's open
  question about whether bulk carries the same consistency semantics as `/upsert`:
  it does, via the operation's completion version.
- **`/delete`, `/compact`, index builds** — also advance the version; a delete
  returns its LSN so "my delete is visible" is expressible.

The LSN is per-namespace and monotonic. Its wire type is an open question below
(raw `u64` vs an opaque string as TopK uses).

### Reads may require an LSN

`/query` (and `/list`) gain two optional parameters, mirroring TopK:

- **`lsn`** — the version from a prior write the caller wants reflected.
- **`consistency`** — `indexed` | `balanced` | `strong` (default `balanced`).

Semantics:

| Level | Guarantee | Cost |
|-------|-----------|------|
| `indexed` | Serve from whatever the index/cache currently reflects; ignore `lsn`. Fast, eventual. | Lowest latency. |
| `balanced` | Serve the latest applied committed version; `lsn` is a floor if supplied but does not block on an in-flight optimize. | Default. |
| `strong` | Block until the read path has applied a version `>= lsn`, then serve; if it cannot within a bound, reject so the client retries. | Read-your-writes; may add latency. |

Because cache-aside already tracks the applied `table_version`, "has the read path
applied `>= lsn`?" is a comparison the read path can already answer; `strong` adds
a bounded wait (or reject-and-retry) around it. No new ordering state.

## Why LSN over timestamp

The pros, streaming first — this is the case for spending the change:

1. **Native to the storage model; zero new ordering machinery.** `table_version`
   is already durable, monotonic, and per-commit. The RFC exposes the engine's own
   sequence number instead of inventing a parallel timestamp and stapling it to
   rows. The entire strip-and-no-op defect class disappears: ordering lives in the
   commit, not in `attributes`.

2. **Streaming ingestion becomes first-class.** A timestamp watermark only makes
   sense for batch commits — "everything before T is durable" needs writes to
   arrive in batches. An LSN assigns a position to *every* commit, so continuous /
   streaming appends get coherent, orderable positions with no batching. One
   primitive covers both patterns: a 10k bulk `/import` is one commit → one LSN; a
   single streaming `/upsert` is one commit → one LSN. This is the on-ramp to a
   streaming write surface and CDC-style ingest, which the batch-timestamp model
   actively fights.

3. **Exact read-your-writes, no clocks.** An LSN is monotonic by construction —
   immune to clock skew, NTP drift, and the intra-batch `_ingested_at` collision
   (`api_scalar_index.rs:146`) that a timestamp cannot escape. `write → lsn`, then
   `query(lsn, consistency="strong")` is a hard guarantee the timestamp watermark
   cannot make.

4. **A consistency dial, not one heuristic bit.** Today the only signal is "is
   `row_count` settled?" — a single boolean approximated by polling. LSN gives the
   graded `indexed` / `balanced` / `strong` levels: eventual, latest-applied, or
   wait-for-my-write, chosen per query. Freshness becomes a caller decision, not a
   gateway guess.

5. **Clean session token across the edge.** The LSN is an opaque cursor the client
   (or Layer) carries forward — "at least as fresh as my last write." Layer's
   `x-layer-stable-as-of` becomes an LSN echoed on writes and accepted on reads;
   no row mutation, no reserved `_hevlayer_*` on the write path, no stripped
   columns. The engine owns ordering; the edge passes the token.

6. **Correct under concurrency / multi-writer.** A single commit log serializes
   all writers regardless of clock skew. A timestamp watermark can advance past
   un-applied concurrent writes and serve stale data *as* "stable" — a silent
   correctness hole. The LSN closes it.

7. **Fails closed, loudly.** Today the gateway injects an inert predicate and fails
   open. A `strong` read either has applied the LSN (serve) or has not (wait, then
   reject/retry) — the safe direction.

## Semantics and constraints to weigh

- **Bounded wait for `strong`.** Blocking until `applied >= lsn` needs a timeout;
  on expiry the engine should reject (a retryable status) rather than serve stale,
  keeping the fail-closed property. The bound and its status code are open below.
- **`indexed` vs `balanced` and the optimize boundary.** A freshly committed row
  is visible to a scan before the ANN index folds it in (the freshness axis in RFC
  0011). `indexed` may reflect the last *optimized* version; `balanced` the last
  *committed* version. The exact mapping of level → (committed vs optimized)
  visibility is a semantics decision this RFC must pin before implementation.
- **Per-namespace scope.** `table_version` is per namespace; the LSN is not global.
  A cross-namespace read (federated query, edge-side fan-out) has no single LSN —
  it carries a vector of them or falls back to `balanced`. This matches TopK's
  per-collection model.
- **Cache coherence is already version-keyed.** `object_cache.rs:24` never caches
  the mutable version-numbered paths (`_versions/`, `_transactions/`, `_latest`)
  and cache-aside follows `table_version` (`service_cache_aside.rs:85`), so reads
  do not need a new invalidation path — the LSN gate rides the existing one.
- **`_ingested_at` stays, for humans.** The engine keeps `_ingested_at` for
  list/scan ordering and "how stale in wall-clock terms" readouts; it is no longer
  load-bearing for read-your-writes. An LSN is opaque — an `lsn → _ingested_at`
  mapping is how a dashboard answers "how far behind am I?"
- **Import async lifecycle.** The LSN a bulk load exposes is the version at
  operation completion, not at `202`. A caller wanting read-your-writes after a
  bulk load waits on `/operations/{id}` for the LSN, then reads `strong` against
  it.

## Interactions with other RFCs

- **RFC 0008 (bulk ingest).** Its open question "should bulk carry the same
  write-stamp / consistency semantics as `/upsert`?" is answered here: the
  operation's completion version *is* the bulk load's LSN. The two write paths
  converge on one consistency primitive.
- **RFC 0011 (recall/build harness).** Its **freshness** axis (insert →
  query-before-optimize → fold) is exactly the `committed`-vs-`optimized` line the
  consistency levels must name; the harness can assert read-your-writes at
  `strong` and measure the `strong` wait as a latency component.
- **RFC 0003 (delete).** Deletes advance the version; an LSN lets "my delete is
  reflected" be required, which the timestamp watermark never expressed.
- **RFC 0086 (Layer edge twin).** Layer stops injecting the inert
  `_hevlayer_upserted_at` predicate on `kind: search`, retires the stripped
  write-stamp for consistency purposes, and maps the LSN onto the inbound wire
  (`x-layer-lsn` on write responses and read requests, a `consistency` hint). The
  store capability matrix row flips from **heuristic / "no LSN"** to **✓ exact,
  LSN-based**. The Turbopuffer backend keeps the timestamp/`updating`-status path,
  so the two kinds diverge on the consistency primitive — which the matrix already
  anticipates.

## Engine vs edge boundary

The engine owns: the durable LSN (the commit version), the read-path gate
(`applied >= lsn`), the `strong` wait/timeout, and the level semantics
(committed vs optimized visibility). Layer owns: surfacing the LSN on the
Turbopuffer-shaped inbound wire, the client token passthrough, and *deciding* a
per-request consistency level. Moving the per-request policy or the token wire
format into the engine would be an edge concern leaking in
(`AGENTS.md` § "The engine/edge test"); moving the wait-until-applied gate into
Layer would be the reverse error — the gateway cannot know when the engine's read
path has applied a version. The current inert predicate injection is exactly that
reverse error and is retired by this RFC.

## Open questions

- **LSN wire type.** Raw monotonic `u64` (what `table_version` already is), or an
  opaque string as TopK exposes (`upsert() → "<lsn>"`)? Opaque strings preserve
  freedom to change the underlying counter; a `u64` is simpler and already what the
  engine returns in `namespace_info`.
- **`strong` timeout + status.** How long does a `strong` read wait before
  rejecting, and with what status (409 / 425 / a typed retryable error), so Layer
  and the SDKs can auto-retry the way TopK's client does?
- **Level → visibility mapping.** Does `balanced` mean last-committed and
  `indexed` mean last-optimized, or another split? This must be pinned against the
  RFC 0011 freshness axis.
- **Cross-namespace reads.** Does a federated/multi-namespace read accept a per-
  namespace LSN vector, or is `strong` single-namespace only in v1?
- **Does `/list` need `strong`,** or is LSN-gated consistency query-path only in
  v1 (list/scan stay `_ingested_at`-ordered eventual reads)?

## References

- `crates/hevsearch-core/src/result.rs:208` — `pub table_version: u64`, the
  per-namespace commit version (the LSN primitive).
- `crates/hevsearch-core/src/object_cache.rs:24,997` — versioned object paths
  (`_versions/`, `_transactions/`, `_latest`) never cached.
- `crates/hevsearch-core/tests/service_cache_aside.rs:85` — read cache-aside
  follows `table_version`.
- `crates/hevsearch-core/tests/manager_import.rs:223–227`,
  `crates/hevsearch-core/tests/manager_namespace_info.rs:109,122` — version
  advances on commit / import.
- `_ingested_at` — engine microsecond write-stamp (`api_scalar_index.rs:146`,
  `api_query_projection.rs:111`), retained for ordering / staleness readout.
- Layer side (the inert path this retires):
  `apps/layer-gateway/src/clients/search.rs:355–361` (strip),
  `apps/layer-gateway/src/routes/query.rs:70,153,246` (inject),
  `site/src/content/docs/kubernetes/store-support.mdx:43` (heuristic / "no LSN").
- RFC 0008 (bulk ingest), RFC 0011 (freshness axis), RFC 0003 (delete) — engine
  neighbors.
- `../../../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the Layer
  twin: wire exposure, consistency policy, the capability-matrix row.
- [TopK LSN-based consistency](https://docs.topk.io/concepts#lsn-based-consistency)
  — the reference model (`upsert() → lsn`; `query(lsn=…, consistency=…)`).
