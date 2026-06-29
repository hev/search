# RFC 0008: Bulk ingest — upsert, import, and the Layer Pipeline write path

Tracking issue: _TBD_

> **Status:** draft, exploration. **Options deliberately kept open.** This RFC
> frames the engine's write-path design space — the relationship between the
> idempotent `/upsert` path and the bulk `/import` path — and how a large first
> load reaches the engine *through hev layer*, which is the only client. It does
> **not** pick a single design yet; it catalogs the options and the constraints so
> the choice is made with the trade-offs written down.
>
> The engine owns the **write-path primitives and their semantics** (engine). The
> **orchestration** of a bulk load — read a Warehouse, embed, drive compact +
> index — is Layer's Pipeline (edge), tracked Layer-side in
> `../../../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md`. This RFC is
> the engine half; it names the Layer half but does not specify it.

## Summary

hev search has two write paths with different cost profiles and different
semantics:

- **`POST /ns/{ns}/upsert`** — JSON body, **latest-write-wins** by `id`
  (merge-insert against the `id` BTree), synchronous, bounded by
  `HEVSEARCH_MAX_BODY_BYTES`. The incremental / idempotent path. Retries and genuine
  updates are both safe.
- **`POST /ns/{ns}/import`** — Arrow IPC stream, **insert-only** (a repeated `id`
  makes a second row), async (`202` + `/operations/{id}`), not bound by the body
  limit, appended in a single Lance commit. The bulk first-load path. It avoids
  JSON's ~3× `f32`-as-decimal inflation and the per-batch commit churn that a long
  run of small `/upsert` calls piles up.

In the standalone-engine framing these were two tools an operator chose between by
hand (the old README "Loading data at scale" recipe: import → compact → index →
query). In the **gateway framing**, the engine has exactly one client — Layer — and
that changes who, if anyone, ever calls `/import`. This RFC works that through.

## Motivation: the gateway has no natural mapping for `/import`

Per RFC 0086, Layer's data plane calls the engine through the portable
`VectorStoreClient` trait. Its write verb is `write_rows`, which maps cleanly onto
`/upsert` (JSON, latest-write-wins — the shape Turbopuffer's wire and Layer's
idempotent document model both expect). `/import` does **not** fit that verb:

- it is **async** (`202` + poll), not the synchronous request/response `write_rows`
  is;
- it is **insert-only**, which contradicts Layer's latest-write-wins document
  model;
- the inbound wire is **Turbopuffer-shaped JSON**, and Turbopuffer has no
  Arrow-IPC bulk endpoint to mirror — exposing `/import` to inbound callers would
  be a non-Turbopuffer extension, against the wire posture.

So as the surfaces stand today, **a bulk first load through Layer would go
batch-by-batch over JSON `/upsert`** — paying exactly the float-inflation and
per-commit costs `/import` was built to avoid. Either that cost is accepted, or
`/import` is reached some other way. That is the open question.

## The options (kept open)

Not mutually exclusive; some compose. Listed engine-first, with the Layer-side
counterpart noted.

1. **Pipeline drives `/import` directly (server-side Arrow).** A large first load
   almost always originates from a Warehouse (Iceberg/Parquet) → embed → write.
   That is Layer's Pipeline, which runs in-cluster and can speak Arrow IPC to the
   engine's admin/data surface directly, bypassing the JSON inbound wire. The
   inbound `write_rows`/`/upsert` path stays for incremental writes; `/import`
   becomes a Pipeline-only sink. **Engine change:** none (or small — confirm the
   Pipeline identity is allowed to reach `/import`). **Layer change:** Pipeline
   learns the Arrow path (RFC 0086 follow-up).

2. **Add a bulk method to the `VectorStoreClient` trait.** A `bulk_write` /
   `import_rows` verb alongside `write_rows`, so bulk is first-class in the shared
   crate and any backend that has a bulk path implements it. **Engine change:**
   none. **Layer change:** trait surface + `SearchClient` impl.

3. **Unify the paths — give `/import` upsert (merge) semantics.** Make the bulk
   path latest-write-wins instead of insert-only, so it is a general efficient
   write path rather than a first-load-only tool. Removes the insert-only footgun
   and the "first load vs update" mode split. **Engine change:** substantial — bulk
   merge-insert against the `id` BTree at import scale, commit/compaction strategy.
   Interacts with the cost argument (merge is more expensive than append).

4. **Accept JSON `/upsert` for everything; document the cost.** Drop `/import`
   from the gateway story entirely and let bulk loads pay the JSON path, sized up
   toward `HEVSEARCH_MAX_BODY_BYTES` with indexes built after. Simplest; the wrong
   call only if first-load wall-clock / cost becomes a real workload constraint.
   **Engine change:** possibly remove `/import` as dead surface (a subtractive
   change to record per `AGENTS.md`).

## Semantics and constraints to weigh

- **Insert-only vs latest-write-wins.** `/import` dups on repeated `id`; `/upsert`
  merges. Any option that routes bulk through `/import` must either guarantee fresh
  ids (true for a first load into an empty namespace) or own a dedup story. Option
  3 dissolves this; options 1–2 inherit it.
- **Commit batching.** `/upsert`'s per-call commit is the scaling cost at first-load
  volume; `/import`'s single-commit append is the win. Whatever path bulk takes,
  the commit count — not the row count — is what hurts on object storage.
- **Async lifecycle.** `/import` returns `202` + `operation_id`; the caller polls
  `/operations/{id}`. The consistency story (RFC 0086 documents the engine's
  watermark as heuristic: `/operations/{id}` + count-settling + write stamps)
  applies here.
- **Orchestration is the operator's, not the engine's.** The recipe's later steps
  — `POST /compact`, then `/index` (IVF_PQ), `/fts-index` (BM25),
  `/scalar-index` — are async engine primitives that Layer's **Index CR** drives
  (RFC 0086 § "Index lifecycle"). The engine exposes them; it does not sequence
  them. This RFC does not change that.
- **Body limits.** `HEVSEARCH_MAX_BODY_BYTES` bounds `/upsert`;
  `HEVSEARCH_IMPORT_MAX_BYTES` (default 8 GiB, `0` disables) and
  `HEVSEARCH_IMPORT_TMP_DIR` bound `/import`. Whichever path survives keeps its
  knob.

## Interactions with other RFCs

- **RFC 0005 (arbitrary string ids).** Both write paths key on `u64` today; Layer's
  document model is string-keyed. Bulk ingest inherits this gap — `/import` cannot
  ship a faithful first load of string-keyed documents until 0005 lands. Any option
  here is gated on it for the real Layer workload.
- **RFC 0086 (Layer-side).** The pipeline (`extract → embed → upsert`), the
  `VectorStoreClient` trait, and the Index CR lifecycle are all there. Options 1
  and 2 are RFC 0086 follow-ups; this RFC is their engine-side anchor.
- **RFC 0003 (delete).** A re-loadable namespace (drop + bulk reload) leans on
  namespace/row delete; relevant if the bulk path is insert-only and reload is the
  update story.

## Engine vs edge boundary

The engine owns: the write-path endpoints, their merge/append semantics, commit
strategy, and the async lifecycle. Layer owns: deciding *when* a load is a bulk
first-load vs an incremental write, reading the Warehouse, embedding, and
sequencing compact + index. This RFC stays on the engine side of that line and
defers the orchestration design to RFC 0086. Adding pipeline orchestration *into*
the engine would be a boundary error (`AGENTS.md` § "The engine/edge test").

## Open questions

- Does `/import` stay insert-only (a first-load tool) or grow merge semantics
  (option 3, a general bulk path)? This is the central fork.
- Is bulk a `VectorStoreClient` trait verb (option 2) or a Pipeline-only
  server-side path (option 1)? — primarily an RFC 0086 call, recorded here.
- If the gateway never drives `/import` (option 4), is the endpoint removed as dead
  surface, or kept for the embedded `hevsearch` package / operator use?
- Should bulk ingest carry the `_ingested_at` / write-stamp semantics identically
  to `/upsert`, so `/list` paging and consistency settling behave the same after a
  bulk load?

## References

- `crates/hevsearch-api/src/handlers.rs` — `/upsert` and `/import` handlers.
- `HEVSEARCH_MAX_BODY_BYTES`, `HEVSEARCH_IMPORT_MAX_BYTES`,
  `HEVSEARCH_IMPORT_TMP_DIR` — the body/byte knobs.
- RFC 0005 (string ids), RFC 0003 (delete) — engine dependencies.
- `../../../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the Layer
  twin: `VectorStoreClient`, the pipeline embed front, the Index CR lifecycle.
