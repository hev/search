# RFC 0016: Native multi-leg query call — N legs, one read cut, per-leg rankings

Tracking issue: [LYR-96](https://linear.app/hevmind/issue/LYR-96/hev-search-rfc-multi-leg-query-call-beyond-the-16-subquery-cap-for)

Motivating workload: Layer `/search` (layer-pro RFC 0116)

> **Status:** Draft (2026-09-18). **Additive, read-path only.** Add
> `POST /ns/{ns}/multi-query`: one request carrying N retrieval legs (BM25,
> fuzzy BM25, ANN), each with its own filter and `k`, all executed against
> **one pinned table version**, returning **per-leg rankings** that the gateway
> fuses. No fusion, no rerank, no cross-namespace. Proposes a hard cap of
> **64 legs** with per-kind sub-caps, derived from this engine's cost model, not
> from Turbopuffer's multi-query limit of 16. Docs only; no implementation in
> this RFC. RFC number 0015 is reserved for a draft that has not landed yet, so
> the README table skips it on purpose.

## Summary

Layer's fused text route expands one search into many subqueries and fuses the
rankings with RRF at the edge. On hev search each subquery is a separate
`POST /ns/{ns}/query` today (`vectorstore-core/src/search.rs:593-690` on
layer-pro `origin/main` @ `249076d`; the store's `multi_ranked_query` is
declared unimplemented, at `:682-690`). That has three costs the engine can remove:

1. **No shared read cut.** Every `/query` derives its own cache generation from
   whatever version the pooled handle holds at that instant
   (`service.rs:240`, `manager.rs:621-636`). An upsert landing between leg 3
   and leg 4 means the gateway fuses rankings from two different table
   versions. The gateway's own stable-read watermark does not help here: it is
   inert on this store (RFC 0012, "fails open").
2. **No shared work.** Each call re-resolves schema, re-analyzes the query
   string (`manager.rs:1735-1739`), re-runs the prefilter, and re-materializes
   `text` + every attribute column for every hit (`manager.rs:1766-1780`), even
   though the fused pool needs each row once.
3. **A budget borrowed from another store.** The expansion is bounded at 15
   fuzzy tokens + 1 BM25 anchor (`routes/hybrid_text.rs:43`,
   `MAX_QUERY_TOKENS`), which RFC 0116 spends as "the upstream cap of 16 per
   call". The gateway's own comment calls the 15 a ranking policy rather than
   an upstream concurrency budget (`:40-42`); either way the number was chosen
   with Turbopuffer in view and applies to every store. On hev search Layer is
   the only client and the engine is ours, so the cap is a choice, and it
   should come from what a leg costs *here*.

The `/search` cascade wants (raw query + up to 3 rewrites) × every full-text
attribute × {BM25, fuzzy} + one ANN leg per embed attribute. Two attributes
already gives 4 × 2 × 2 + 2 = 18. The exact fan-out arithmetic is owned by the
RFC 0116 amendment (LYR-91), which had not landed when this was written; this
RFC is written against layer-pro `main` and the numbers here follow the
amendment once it lands. The cap below is sized with headroom for that reason.

## What the engine can and cannot express today

Two facts shape the design and should be read before the wire:

- **One text surface, one vector column per namespace.** The schema has a
  single `text` column (analyzed into `text_tok`, RFC 0001) and a single
  `vector` column; `FtsTarget::column()` is hardwired to `text_tok`
  (`manager.rs:2895-2900`). The Layer client maps only the `text` attribute
  onto it and ignores the `rank_by` field name
  (`vectorstore-core/src/search.rs:663-670`). So "BM25 per attribute" and "ANN
  per vector column" are **addressable in this call's wire but reject anything
  other than `text` / `vector` in v1.** Multi-column FTS and multi-vector-column
  namespaces are a separate, prerequisite RFC; this one fixes the call shape so
  that RFC does not need a wire change.
- **Per-token fuzzy legs collapse on this engine.** The gateway builds one fuzzy
  leg per token: BM25-ranked over the full input, filtered by
  `[field, "Fuzzy", token, …]` (`routes/hybrid_text.rs:557-590`). The search
  client keeps only the `max_edit_distance` from that filter and discards the
  token (`vectorstore-core/src/search.rs:1097-1149`), so every fuzzy leg of one
  expansion becomes the *same* engine request:
  `{text: <full input>, fuzzy: {max_edit_distance}}`. The engine already does
  the per-token work inside one query: `full_text_query` expands to one
  `should` clause per token with a ladder-bounded distance
  (`manager.rs:2943-2984`, ladder at `:2868-2874`). **On hev search the fuzzy
  unit is one leg per (query string, field), not one per token.** A gateway
  that spends 15 of 16 slots on per-token fuzzy legs here buys 15 copies of one
  ranking.

## Proposal

### Request

```jsonc
POST /ns/{namespace}/multi-query
{
  "legs": [
    { "id": "bm25:text:q0",  "kind": "text",   "field": "text",
      "text": "vitamin d bone density", "k": 100,
      "filter": "year >= 2015" },
    { "id": "fuzzy:text:q0", "kind": "text",   "field": "text",
      "text": "vitamin d bone density", "k": 100,
      "fuzzy": { "max_edit_distance": 0 },   // what the gateway clamp sends today
      "filter": "year >= 2015" },
    { "id": "ann:vector:q0", "kind": "vector", "column": "vector",
      "vector": [0.12, -0.03 /* … */], "k": 100, "nprobes": 20,
      "filter": "year >= 2015" }
  ],
  "hydrate": { "rows": true, "include_vector": false },
  "on_leg_error": "partial",          // "partial" (default) | "fail"
  "lsn": 42, "consistency": "balanced" // RFC 0012; accepted once that lands
}
```

- **`legs[].id`** — caller-chosen label, unique within the request, echoed
  back. The engine never interprets it.
- **`kind: "text"`** — `text` (required), `field` (v1: `"text"` only), optional
  `fuzzy` exactly as on `/query` (`query.rs:93-108`). A text leg with no
  `fuzzy`, or an effective distance of 0, is the BM25 leg.
- **`kind: "vector"`** — exactly one of `vector` / `vectors` (same rules as
  `/query`, `query.rs:37-48`), `column` (v1: `"vector"` only), optional
  `nprobes` or `exact` (RFC 0014; mutually exclusive, `query.rs:182-195`).
- **`filter`, `k`** — per leg. Same DataFusion SQL dialect and prefilter
  semantics as `/query`.
- **No hybrid leg.** A leg is single-modality. `/query`'s in-engine hybrid
  (RRF inside LanceDB, `manager.rs:1781-1783`) stays on `/query`; here fusion
  is the caller's.
- **`hydrate`** — see "Hydrate once" below.

### Response

```jsonc
{
  "cut": { "table_version": 42, "last_write_ms": 1789000000000 },
  "legs": [
    { "id": "bm25:text:q0", "status": "ok", "source": "backend",
      "k": 100, "returned": 100, "truncated": true, "took_ms": 6,
      "hits": [ { "id": "PMC5461234", "score": 11.2, "rank": 1 } /* … */ ] },
    { "id": "fuzzy:text:q0", "status": "ok", "source": "collapsed",
      "collapsed_into": "bm25:text:q0", "k": 100, "returned": 100,
      "truncated": true, "took_ms": 0, "hits": [ /* same ranking */ ] },
    { "id": "ann:vector:q0", "status": "error",
      "error": { "code": "backend", "message": "query.execute: …" } }
  ],
  "rows": { "PMC5461234": { "text": "…", "attributes": { "year": 2019 } } },
  "partial": true,
  "took_ms": 31
}
```

- **`cut`** — the single Lance `table_version` (`result.rs:208`) every leg ran
  against, plus `last_write_ms`, that version's commit timestamp. This is RFC 0012's LSN; the gateway can carry it as the response's
  consistency token with no further call.
- **`hits`** — `id`, raw `score` (BM25 `_score` or `_distance`; not comparable
  across kinds, as today), and 1-based `rank`. Rank is explicit so the gateway's
  RRF does not depend on array position surviving a proxy.
- **`truncated`** — `returned == k`: the leg filled its budget and more matches
  may exist. `false` means the leg is exhaustive under its filter. This is the
  signal the edge needs to tell "pool is complete" from "pool was cut".
- **`source`** — `backend`, `exact_cache`, `deduped` (byte-identical to an
  earlier leg; carries `deduped_from`), or `collapsed` (see the fuzzy section).
- **`took_ms`** — per leg, wall-clock inside the engine; top-level is the whole
  request.

### One stable-read cut

The handler resolves the cut **once**, before any leg runs:

1. Resolve the table handle and read `(version, timestamp_nanos)` from its
   manifest — the same pair `generation_of` hashes (`manager.rs:641-655`).
2. If RFC 0012 has landed and the request carries `lsn`/`consistency`, apply
   the `applied >= lsn` gate here, once, not per leg.
3. Pin that version for every leg. lancedb `=0.29.0` exposes
   `Table::checkout(version)` (`table.rs:1042`), but it switches the handle it
   is called on into time-travel mode, and handles are pooled and shared
   (`manager.rs:805-807`). The multi-leg path must pin a **request-scoped**
   handle, never the pooled one.
4. Derive the cache generation from the pinned pair and use it for every leg's
   lookup and populate.

Point 4 needs one small cache change. `NamespaceCache::try_get` re-reads the
namespace's *current* generation on every call (`cache/layer.rs:109-115`), and
a concurrent `/query` may `set_generation` forward mid-request
(`service.rs:241`). The multi-leg path needs a `try_get_at(ns, generation,
hash)` that takes the pinned generation explicitly;
`populate_with_generation` already does (`cache/layer.rs:134-147`).

The guarantee: **all legs of one response observe exactly one committed table
version, and the response names it.** Two requests are not tied to each other;
that is RFC 0012's job.

### Hydrate once

Today each `/query` hit materializes `id`, `text`, `_ingested_at` and every
attribute column (`manager.rs:1766-1780`). Fused pools overlap heavily (that
overlap *is* the RRF signal), so N legs pay for the same row up to N times.
Here legs project `id` + score only, and with `hydrate.rows: true` the engine
does **one** take over the union of returned ids at the pinned version and
returns it as the top-level `rows` map. `hydrate.rows: false` returns rankings
only, for a caller that hydrates the fused top of the pool itself.

### Errors and partial failure

Request-level, nothing executes, HTTP 400:

| Condition | Code |
|---|---|
| `legs` empty, over the hard cap, or over a per-kind sub-cap | `leg_budget_exceeded` (body names the limit hit and the count) |
| duplicate `legs[].id` | `duplicate_leg_id` |
| `field` ≠ `text` / `column` ≠ `vector` (v1), wrong vector shape or dim for the namespace, `exact` + `nprobes`, `k` out of range | `invalid_leg` (body names the leg id) |
| a leg's `filter` fails to plan | `invalid_filter` (names the leg id). Deterministic caller error, so it fails the request in both modes, matching `/query`'s mapping (`manager.rs:1832-1838`). |

Runtime, per leg: a leg that fails in the backend gets `status: "error"` with a
`code`/`message`; the others are unaffected.

- `on_leg_error: "partial"` (default) — HTTP 200, `partial: true`, failed legs
  carry no `hits`. If **every** leg fails, HTTP 502 with the per-leg errors.
- `on_leg_error: "fail"` — first failure cancels outstanding legs; HTTP 502.

A namespace with no table returns 200 with every leg `ok`, `returned: 0`, and
`cut.table_version: 0`, mirroring `/query`'s empty result
(`manager.rs:1653-1661`). A text leg whose analysis yields no tokens is `ok`
with zero hits (`manager.rs:1740-1745`), not an error. There is no per-leg
timeout knob in v1; the gateway's request timeout bounds the call.

### Result cache (foyer)

- **Per-leg entries, no whole-request entry.** A request's leg set varies with
  rewrites and attributes; its legs repeat. The raw-query BM25 leg recurs across
  requests where the rewrites do not.
- **Key** = `(namespace, pinned generation, leg hash)` — the existing
  `CacheKey` (`cache/key.rs:30-37`). The leg hash is a bincode-canonical struct
  like `hash_query_for_cache` (`service.rs:434-463`) with a `kind: "leg/v1"`
  discriminator, the way facets discriminate (`service.rs:480`), plus `field` /
  `column`. `legs[].id` is a label and is **not** in the key. `hydrate` is not
  in the key either.
- **Payload** = ids + scores only. Leg entries deliberately do not share with
  `/query` entries, whose payload carries `text`/attributes/vector
  (`service.rs:496-498`). Small payloads are the point: 64 legs × 200 hits is
  tens of KB, not MB.
- **Hydration is not result-cached.** The union take reads Lance data
  fragments, which the NVMe object cache already serves
  (`object_cache.rs:17-27`).
- **In-request dedupe.** Legs with equal hashes execute once; the rest report
  `source: "deduped"`.
- Invalidation is unchanged: a commit moves the generation and old leg entries
  become unreachable.

## The cap

### What a leg costs here

Evidence from the repo (single-vector, 1536-dim class workloads; the
multivector case is called out separately):

| Path | Measured | Source |
|---|---|---|
| exact-cache hit | 56–62 µs p50 | `bench/results/cold_vs_warm_realistic.md:16-19` |
| backend ANN query, warm handle, novel query (S3, 1M rows) | 7.2 ms p50, 194 ms p95 | `bench/results/first_query_profile_objcache.md:24` |
| backend IVF_PQ, cold (MinIO, 100k rows) | 71 ms p50 | `bench/results/cold_vs_warm_realistic.md:18` |
| backend ANN query, cold process / fresh process (S3, 1M rows) | 383 ms / 144 ms p50 | `bench/results/first_query_profile_objcache.md:22,26` |
| cold IVF_PQ object reads | ~140 small GETs, request-count-bound | `object_cache.rs:3-5` |
| multivector MaxSim | CPU-bound, ~17 s p50 (~46 s at 1M docs), cache does not help | `bench/results/beir_multivector_objcache.md:221,265,413-420` |

Per leg, unshared: one Lance plan + execution, a top-`k` heap, and the
index reads for that modality — IVF centroids + `nprobes` (default 20,
`query.rs:14`) PQ partitions for ANN; posting lists per token for BM25; for
fuzzy, a term-dictionary expansion per token at distance ≤ 2 before the
postings. After the first touch those byte ranges are NVMe-local.

### What is shared across legs of one request

| Shared | Per | v1 |
|---|---|---|
| table snapshot, schema info, handle resolution | request | yes |
| LSN / consistency gate (RFC 0012) | request | yes |
| analyzer pass (`word_v4`) | distinct `text` string | yes — BM25 and fuzzy legs over one string analyze once |
| execution of identical legs | distinct leg hash | yes (dedupe) |
| row materialization (`text` + attributes) | request (union of ids) | yes — the largest saving |
| index byte ranges (centroids, PQ partitions, postings, term dictionary) | process, via the object cache and Lance's session | yes, as today |
| ANN partition selection | distinct query vector | no gain in v1: each ANN leg has its own vector, so probes are not shareable; only the centroid table is |
| prefilter evaluation | distinct `filter` string | **not in v1.** lancedb's builder takes `only_if` per query (`manager.rs:1826-1828`, `:1846-1848`) and does not expose a reusable row mask. Legs sharing a filter share the scalar-index and fragment reads through the object cache, not the evaluated mask. A shared-mask optimization needs to drop below the lancedb builder; listed under open questions. The wire already lets the engine detect "same filter on every leg" by string equality, so it is addable without a wire change. |

Being straight about the last row matters for the cap: the cost model below
assumes a leg pays its own prefilter.

### The proposal

| Limit | Value | Why |
|---|---|---|
| **Hard cap, legs per request** | **64** | With in-request execution concurrency 8, 64 legs is 8 waves. If every leg lands at the 7.2 ms warm p50 that is ≈ 57 ms of leg time, inside RFC 0116's illustrative `legs_ms: 88`; a wave ends at its slowest leg, so one 194 ms tail leg puts the request near 250 ms (see below). 128 legs would double both and buy nothing the workload asks for: (4 strings × 4 text fields × 2) + 8 ANN = 40, which is 5 waves. The 24 spare legs are headroom for the LYR-91 fan-out and can only be BM25. |
| **Sub-cap, vector legs** | **8** single-vector; **1** on a multivector namespace | The expensive kind. Each probes 20 partitions and is the only leg whose cold cost is request-count-bound (~140 GETs). MaxSim is CPU-bound at seconds per query, so parallel MaxSim legs just contend. |
| **Sub-cap, fuzzy text legs** (effective distance > 0) | **16** | Term-dictionary expansion per token makes a fuzzy leg the costliest text leg, and that cost is unmeasured, so this limit gets no headroom: 16 = 4 strings × 4 fields, exactly the fuzzy share of the 40-leg sizing workload. |
| **Per-leg `k`** | 1..=1000 | The gateway's per-leg ceiling is 200 (`routes/hybrid_text.rs:51`); 1000 leaves room without allowing scan-sized legs. |
| **Σ `k` across legs** | ≤ 12,800 (= 64 × 200) | Bounds the union take and the response. |
| **Execution concurrency** | 8 per request, engine config, not wire | Keeps one request from monopolizing the runtime. Dedupe and cache hits do not consume a slot. |

BM25 legs have no sub-cap of their own; they are bounded by the hard cap.

**What the cap is not based on:** Turbopuffer's multi-query limit of 16, or the
gateway's `MAX_QUERY_TOKENS` ranking policy. Neither describes this engine.
The caps are constants to start, and they count *submitted* legs (before dedupe
and collapse) so the budget is predictable from the request alone.

The field count in the sizing workload is forward-looking: a namespace has one
text surface today, so the largest request the edge can build now is
(4 × 1 × 2) + 1 ANN = 9 legs. Four fields is the assumption both the hard cap
and the fuzzy sub-cap are sized on; a namespace with more fuzzy-worthy fields
than that waits for a measured fuzzy leg cost rather than a bigger guess.

How far the numbers go: the 7.2 ms / 194 ms row is one sequential single-vector
IVF_PQ ANN query (1M rows, dim 1536, `k`=10, `nprobes`=20, S3 eu-west-1, object
cache on), 19 samples at 6.5–8.8 ms and one at 193.86 ms. It is not a text leg,
not `k` = 100–200, and not 8-way concurrent. At a 1-in-20 outlier rate a 64-leg
request draws at least one tail leg about 96% of the time (40 legs: 87%), so
≈ 250 ms, not ≈ 57 ms, is the figure to plan on. Cold, on that same S3 bench,
the first wave pays 383 ms p50 in a cold process (144 ms in a fresh one,
`first_query_profile_objcache.md:22,26`); later waves read NVMe-warm ranges and
are unmeasured. **Text-leg cost is argued from mechanism, not measured:** there
is no FTS or fuzzy bench in `bench/results/`, yet 32 of the 40 sized legs are
text legs. RFC 0011's harness should gain a multi-leg scenario with text legs
before these constants are treated as settled; see open questions.

## Interaction with the fuzzy clamp

**Status as found (2026-09-18).** hev/search#4 ("Fuzzy FTS matches all documents
for edit distance ≥ 1 and `auto`") is **closed**, completed 2026-07-02: bounded
per-token fuzzy and the `auto` ladder shipped as RFC 0004
(`manager.rs:2934-2984`). RFC 0116's store matrix (`:401`) and the public query
docs (`site/src/content/docs/api/query.mdx:294,504`) still say the clamp holds
"while hev/search#4 stands". That wording is stale. What actually still stands
is the **gateway's interim clamp** (`routes/hybrid_text.rs:1386-1400`):
`fuzziness: "auto"` becomes `Fixed(0)` on a search-store namespace, while an
explicit numeric fuzziness is forwarded. Lifting it is pending
[hev/search#9](https://github.com/hev/search/issues/9) (open; needs an image
deploy and a reindex, reserved to the operator) and a verify gate. **This RFC
does not propose lifting the clamp.** It specifies both cases:

- **Clamp in force.** A fuzzy leg arrives with effective distance 0. By
  construction that is the plain BM25 query (`manager.rs:2958-2961`), so it is
  byte-identical in result to the BM25 leg over the same string, field, filter
  and `k`. The engine **collapses** it: executes once, reports the second leg
  as `source: "collapsed"` with `collapsed_into`. It still counts against the
  hard cap (budget is predictable from the request) but not against the fuzzy
  sub-cap (its effective distance is 0). The right edge behaviour is to **not
  send these legs at all** and spend the budget on rewrites and fields; the
  collapse is a safety net, not the plan. The same collapse applies when
  `auto` yields distance 0 for every token (all tokens ≤ 5 chars,
  `manager.rs:2966-2970`).
- **Clamp lifted.** Fuzzy variants are real legs with their own rankings: one
  per (query string, field), `fuzzy.max_edit_distance: "auto"` or numeric. They
  count against the fuzzy sub-cap of 16. Still not one per token.

Either way the gateway should stop emitting per-token fuzzy legs to this store.

## Gateway changes (for the edge to file against)

Self-contained; everything here is Layer's to build.

1. **Implement `multi_ranked_query` for the search store** as one
   `POST /ns/{ns}/multi-query`, replacing the N × `ranked_query` loop for that
   store. RRF (`rrf_fuse_legs`, rank constant 60) stays in the gateway, fed
   from `legs[].hits[].rank`.
2. **Advertise a numeric leg budget per store.** The likely seam is the runtime
   capability report (LYR-86; provisionally layer-pro RFC 0117, not landed —
   this RFC names what it should carry and does not design it):
   - `max_legs` — hev search 64; Turbopuffer 16.
   - per-kind limits — hev search `max_vector_legs: 8` (`1` multivector),
     `max_fuzzy_legs: 16`.
   - `stable_cut` — hev search `true` (one `table_version` per response);
     Turbopuffer `false` (the gateway watermark keeps doing that job there).
   - `fuzzy_leg_unit` — hev search `query_string`; Turbopuffer `token`.
   - `max_leg_k` — hev search 1000.
3. **Spend the budget per store.** Turbopuffer: exactly today's order (ANN, BM25
   per attribute, per-token fuzzy round-robin, drop at 16). hev search: ANN per
   embed attribute; BM25 per (query string, field); fuzzy per (query string,
   field) only when the clamp is lifted, none while it is in force; no
   per-token legs. `hybrid.dropped_legs` keeps its meaning.
4. **Echo.** Map `cut.table_version` to the consistency header, per-leg
   `took_ms` / `truncated` / `source` into `hybrid.legs`.
5. **Sharded namespaces.** Shards are separate engine namespaces, so the call
   is one request per shard, each with its own cut. A cross-shard cut is
   cross-namespace and out of scope.
6. **Unchanged:** the Turbopuffer path, byte-for-byte — same legs, same cap,
   same wire. `/query` on the engine. The clamp. Reranking.

Until a namespace can hold more than one text field or vector column, the edge
addresses `field: "text"` / `column: "vector"` only.

## Non-goals

- **Cross-namespace** (and therefore cross-shard) legs or cuts.
- **Reranking.** Layer's, RFC 0116.
- **Any change to the Turbopuffer path.**
- **Fusion in the engine for this call.** RFC 0086 splits it: the engine
  returns rankings, the edge fuses. `/query`'s existing in-engine hybrid is
  untouched and is not a leg kind.
- **Multi-column FTS / multiple vector columns.** Prerequisite work with its
  own RFC; this call only reserves the addressing.
- **Lifting the fuzzy clamp** (hev/search#9).
- **Auth, tenancy, per-caller quotas.** The caps are engine resource bounds, not
  tenant policy (`AGENTS.md`, engine/edge test).

## Interactions with other RFCs

- **RFC 0012 (LSN).** The cut *is* an LSN read. This call is the first place the
  engine returns `table_version` on a query response; `lsn`/`consistency` on the request
  are accepted once 0012 lands and gate once per request.
- **RFC 0004 (fuzzy).** Reused unchanged per leg; motivates the fuzzy leg unit.
- **RFC 0014 (exact).** `exact` is a per-leg knob on vector legs, in the leg
  hash, as it is in `/query`'s.
- **RFC 0009 (HNSW / `refine_factor`).** New ANN knobs become per-leg fields.
- **RFC 0011 (bench harness).** Should gain a multi-leg scenario: legs × cache
  state × concurrency, to validate the constants above.
- **RFC 0005 (string ids).** `hits[].id` and the `rows` keys are `RowId`.

## Engine vs edge boundary

Executing retrieval plans against one snapshot, sharing work between them, and
bounding their cost is engine. Choosing the legs, rewriting queries, fusing,
reranking, and deciding how much budget a tenant may spend is edge. The engine
interprets no leg label and applies no fusion.

## Open questions

1. **Shared prefilter mask.** Worth dropping below the lancedb builder to
   evaluate a filter once per distinct string? Only if the multi-leg bench
   shows prefilter dominating; `/search` replicates one filter to every leg, so
   the upside is real.
2. **Request-scoped pinned handle cost.** Does `Table::clone()` share the
   dataset wrapper that `checkout` mutates? If so the pin needs a second open
   per request (warm manifest, but not free) or a small pinned-handle pool keyed
   by version.
3. **Are the constants right?** They come from single-leg benches. Should they
   be env-configurable from day one, or constants until a bench says otherwise?
4. **Scores across legs.** Should hits carry a normalized score as well as raw?
   Proposed no: RRF needs rank only, and normalization is fusion policy.
5. **Per-leg timeout / deadline propagation** from the gateway.

## References

- `crates/hevsearch-core/src/service.rs:223-303` — `/query` cache-aside path;
  `:434-463` cache-key canonical form.
- `crates/hevsearch-core/src/manager.rs:621-655` — generation from
  `(version, timestamp_nanos)`; `:1640-1875` — query construction;
  `:2868-2984` — fuzzy ladder and FTS query builder.
- `crates/hevsearch-core/src/cache/layer.rs:109-147`, `cache/key.rs:30-37` —
  result-cache key and lookup/populate.
- `crates/hevsearch-core/src/object_cache.rs:1-27` — byte-range cache and the
  cold-query GET profile.
- `crates/hevsearch-core/src/query.rs:14,37-108,182-195` — request types.
- `crates/hevsearch-api/src/lib.rs:34`, `handlers.rs:226-246` — `/query` route.
- layer-pro `origin/main` @ `249076d`:
  `apps/layer-gateway/src/routes/hybrid_text.rs:40-51,557-590,1386-1400`;
  `crates/vectorstore-core/src/search.rs:593-690,1097-1149`;
  `docs/rfcs/0116-search-endpoint-fused-rerank.md:401`.
- [RFC 0004](0004-fuzzy-fts.md), [RFC 0012](0012-lsn-read-consistency.md),
  [RFC 0014](0014-exact-knn-query-mode.md),
  [RFC 0011](0011-recall-and-build-benchmark-harness.md); Layer RFC 0086, 0116.
