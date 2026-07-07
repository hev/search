# RFC 0013: Delta-friendly small writes — group commit, WAL buffering, and an auto-compaction policy

> **Status:** draft, exploration (options kept open). This RFC frames the
> small-write amplification problem — every `/upsert` batch is its own Lance
> commit, producing a tiny fragment + manifest + transaction file, with
> documented throughput decay — and catalogs the mitigation options: group
> commit, an engine-side WAL/memtable, and an auto-compaction policy. It does
> not pick one; the streaming-ingest workload (RFC 0012's on-ramp) is the
> motivating case, and the comparative reference is
> [LodeDB](https://github.com/Egoist-Machines/LodeDB)'s delta persistence,
> which keeps commits O(changed rows) at any corpus size.

## Summary

The engine's write path is commit-per-request. `upsert_with_distance_metric`
executes one `merge_insert` per call (`manager.rs:1054-1061`), and each
execution is a full Lance storage commit: a new data fragment, a new
`_versions/N.manifest`, and a transaction file — regardless of whether the
batch carried one row or ten thousand. The bench notes record the consequence:
per-batch upsert sustains **~2–6 docs/s** on a 382k-doc corpus ("each batch is
its own storage commit, and the per-commit and small-fragment bookkeeping makes
throughput decay as the namespace grows",
`bench/results/beir_multivector_objcache.md:372-374`), while the single-commit
`/import` path moves the same corpus at **~2,830 docs/s** — three orders of
magnitude apart. `docs/architecture.html:208` states it plainly: "After many
small upserts, the namespace accumulates many small files."

Today the only remedy is **manual** `/compact` (202 + background
`tokio::spawn`, `handlers.rs:712,726-746`; `OptimizeAction::default()` at
`manager.rs:2178-2181`, 1M-row target fragments) — there is no auto-compaction
policy, and old-version cleanup is deliberately skipped on the import path
(`skip_auto_cleanup`, `manager.rs:1294-1296`) with no scheduled sweep to
compensate.

This RFC proposes closing the gap between "one row" and "one commit" so a
streaming / trickle write pattern is viable, and making compaction a policy
instead of an operator chore.

## Motivation: the amplification is structural, and streaming makes it worse

Three compounding costs per small upsert:

1. **Commit overhead is fixed per call.** Manifest + transaction + fragment
   writes to object storage dominate a small batch. A 1-row upsert pays the
   same S3 round-trips as a 10k-row one.
2. **Merge-insert classification scales with table size.** `merge_insert`
   keyed on `id` must classify incoming ids as update-vs-insert; the auto-built
   `id` BTree (`manager.rs:1072-1112`) bounds this, but the per-commit
   bookkeeping still grows with fragment count.
3. **Tiny fragments degrade the read path until someone compacts.** Newly
   committed rows are unindexed and brute-force scanned until an optimize
   absorbs them (`docs/api.html:243,260`); many tiny fragments multiply
   per-query fragment visits and cache entries. With no auto-compaction, decay
   is unbounded between manual interventions — and "an undocumented manual fix
   is a process failure" is the house rule this violates.

RFC 0012 makes this urgent rather than latent: it frames the LSN as "the
on-ramp to a streaming write surface" where *every* commit gets a sequence
number. A streaming surface built on commit-per-row inherits all three costs at
row granularity. The write path must get cheaper before the streaming surface
makes it hotter.

### The LodeDB reference point

LodeDB (embedded, local-first — architecturally the opposite of this engine,
see the fork-choice discussion) demonstrates the target *shape*, not the
target *implementation*:

- **Commits are O(changed rows).** A delta journal (`.tvd`) appends only
  modified row encodings; a 1k-row commit stays sub-millisecond at any corpus
  size (claimed **1,308×** vs full rewrite at 1M vectors: 0.31 ms vs 404.9 ms).
- **A WAL decouples durability from publication.** Default mode appends one
  framed record per `add`/`remove` to a `.wal` ("a durable single add costs
  roughly an order of magnitude less than publishing a whole generation per
  write"); periodic checkpoints fold the WAL into a committed generation and
  truncate.
- **Atomic generation pointer.** A commit spans several files but is sealed by
  atomically swapping one `commit.json` root pointer over generation-addressed
  artifacts — crash mid-commit rolls back to the last committed generation.

Lance already gives us the third property (versioned manifests + `_latest` are
exactly a generation pointer, `object_cache.rs:24,997`) and half the first
(fragments *are* deltas; deletes are deletion vectors, not rewrites, RFC 0003).
What we lack is the second: any decoupling of "durable" from "committed" for
small writes.

## Option space

### A. Group commit (coalesce concurrent upserts)

Hold a short window (or until a byte/row threshold) and fold concurrent
`/upsert` requests into one `merge_insert` commit. Callers block until their
group's commit lands, then all receive the same LSN.

- *Pros:* no new durability machinery; no change to crash semantics (nothing
  is durable before the Lance commit, same as today); LSN semantics (RFC 0012)
  unchanged — one commit, one LSN, shared by the group. Smallest option.
- *Cons:* only helps under **concurrent** writers; a single trickle writer
  (one row per second) still pays commit-per-row unless the window adds
  latency. Duplicate-id-within-request validation (`manager.rs:939-947`) must
  become duplicate-id-within-group handling (latest-write-wins ordering inside
  the group, or split the group).

### B. Engine-side WAL / memtable (the LodeDB shape)

Append small upserts to a durable log (local NVMe — the L2 cache device
already exists — or an object-storage append target), acknowledge once logged,
and fold the log into a real Lance commit on a checkpoint cadence
(rows/bytes/time). Reads merge the memtable overlay with the committed table,
or accept bounded staleness at the default consistency level.

- *Pros:* the real fix — a durable single-row write costs a log append, not a
  commit; checkpoint commits are large and well-shaped (fewer, bigger
  fragments); this is the write path a streaming surface actually needs.
- *Cons:* the expensive option. It splits "durable" from "readable-committed",
  which **interacts directly with RFC 0012**: does a WAL-acked write get an
  LSN before its checkpoint commit exists? (TopK-style answer: the LSN is
  assigned at log append; `strong` reads wait for *application*, which now
  means checkpoint-or-overlay.) It also introduces node-local durable state
  into an engine whose moat is statelessness over object storage — a WAL on
  NVMe means a pod loss can lose acked writes unless the log is replicated or
  object-storage-backed. Read-path overlay is real complexity (filter, FTS,
  and vector scoring over uncommitted rows).

### C. Auto-compaction policy (necessary under every option)

Trigger the existing `manager.compact` path automatically from a policy —
e.g. fragment-count threshold, small-fragment ratio, or a post-write debounce —
instead of waiting for an operator to notice a growing `fragment_count`
(`docs/api.html:450`). Add the old-version cleanup sweep that
`skip_auto_cleanup` currently defers forever.

- *Pros:* bounds read-path decay regardless of write-path choice; cheap to
  ship (the mechanism exists, only the trigger is missing); pairs with RFC
  0011's build-cost axis (compaction is a build-memory event — it must respect
  the same RAM budget the harness measures, and today it runs as an in-process
  `tokio::spawn` that can pressure the serving pod).
- *Cons:* not sufficient alone — it cleans up after amplification rather than
  preventing it; a compaction storm under sustained trickle writes is its own
  failure mode (policy needs hysteresis).

### D. Accept it; push batching to the edge

Document that small upserts are expensive and have Layer's Pipeline batch
writes before they reach the engine.

- *Pros:* zero engine change.
- *Cons:* fails the boundary test in the other direction — write shaping *at
  the data* is engine territory (the engine knows fragment sizes, commit
  costs, and compaction state; the gateway doesn't), and it forfeits the
  streaming-ingest direction RFC 0012 opens. Kept for completeness, not
  favored.

The likely landing: **C is table stakes under any outcome; A is the cheap
first step; B is the streaming end-state**, adopted only when a real streaming
workload (RFC 0012's CDC framing, or a demo that needs per-row ingest) forces
it. This RFC keeps that sequencing open.

## Interactions with other RFCs

- **RFC 0012 (LSN consistency).** Group commit is LSN-neutral (one commit, one
  LSN). A WAL is not: the RFC must decide whether LSNs are assigned at log
  append or at checkpoint, and what `strong` waits on. Whichever option lands
  must not break `write → lsn → query(strong)`.
- **RFC 0008 (bulk ingest).** `/import` already has the right shape (whole
  stream = one commit, `manager_import.rs:207-229`); this RFC is about the
  writes that *can't* be batched by the caller. The two together cover the
  write-pattern spectrum.
- **RFC 0011 (recall/build harness).** The freshness axis
  (insert → query-before-optimize → fold) measures exactly the window this RFC
  manipulates; auto-compaction (C) and checkpointing (B) both move the fold
  point. The harness should gain a **sustained-trickle** scenario: N rows/s
  for M minutes, tracking fragment count, query latency decay, and (post-fix)
  the policy's containment of both.
- **RFC 0003 (delete).** Deletes are already delta-shaped (deletion vectors);
  the WAL option must journal deletes too, or exempt them (they are cheap
  commits already).

## Engine vs edge boundary

Write shaping at the data — commit coalescing, the WAL, checkpoint cadence,
compaction policy — is engine-owned: it depends on fragment sizes, commit
costs, and index state only the engine sees. Layer owns *client-side* batching
guidance and the Pipeline's batch sizing, and carries the LSN/consistency
token unchanged (RFC 0012's split). Option D (making the gateway responsible
for the engine's write economics) is the boundary error this section exists to
name.

## Open questions

- **Where does a WAL live?** Local NVMe (fast, but node-local durable state —
  acked-write loss on pod loss), object storage (durable, but an append-heavy
  small-object pattern S3 dislikes), or replicated? This is the crux of
  option B and probably decides its fate.
- **LSN assignment under B** — at log append or at checkpoint? (See RFC 0012
  interaction.)
- **Group-commit window** — fixed (e.g. 5–20 ms), adaptive, or
  threshold-only? What does it do to p50 upsert latency for the lone writer?
- **Auto-compaction trigger** — fragment count, small-fragment ratio, bytes
  of un-compacted deltas, or debounced post-write? What hysteresis prevents
  compaction storms under sustained trickle?
- **Does compaction move off the serving process** first (RFC 0011's
  build-off-process knob), so an auto-policy can't OOM the serving pod?
- **Fragment-size knob.** Compaction targets Lance's default 1M rows/fragment
  (`manager.rs:2154-2155`) with no override; does this RFC surface one?

## References

- `crates/hevsearch-core/src/manager.rs:1054-1061` — one `merge_insert` per
  upsert = one commit; `:939-947` duplicate-id rejection; `:1072-1112`
  first-write `id` BTree (a second commit).
- `crates/hevsearch-core/src/manager.rs:1292-1309` — import: one append
  commit, `skip_auto_cleanup: true` (`:1294-1296`).
- `crates/hevsearch-core/src/manager.rs:2151-2196` — `/compact`:
  `OptimizeAction::default()`, 1M-row target, handle eviction;
  `crates/hevsearch-api/src/handlers.rs:712,726-746` — 202 + background spawn,
  manual-only.
- `bench/results/beir_multivector_objcache.md:372-390` — 2–6 docs/s per-batch
  upsert vs ~2,830 docs/s single-commit import; the motivating numbers.
- `docs/architecture.html:208,213` — small-fragment accumulation; compaction
  target.
- `crates/hevsearch-core/tests/manager_import.rs:207-229` — whole stream = one
  commit (version +1).
- [LodeDB](https://github.com/Egoist-Machines/LodeDB) — delta persistence
  reference: O(changed) `.tvd` delta journal, `.wal` + checkpoint fold,
  atomic `commit.json` generation pointer (claimed 1,308× at 1M vectors).
- RFC 0012 (LSN), RFC 0011 (freshness + build axes), RFC 0008 (bulk ingest),
  RFC 0003 (delete) — engine neighbors.
