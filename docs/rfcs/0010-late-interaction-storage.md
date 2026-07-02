# RFC 0010: Late-interaction storage — half-precision token bags and the Iso-ModernColBERT reference

Tracking issue: _TBD_

> **Status:** draft, proposal. **Additive engine capability.** The engine already
> serves ColBERT-style late interaction: a `VectorKind::Multivector` namespace
> stores a bag of per-token sub-vectors and scores them with MaxSim through
> LanceDB's late-interaction query plan (`vector.rs:22-30`; `manager.rs:1664-1693`,
> the `nearest_to` + `add_query_vector` loop). What it does **not** do is store that
> bag economically: the inner sub-vector item is **hardcoded to `Float32`** at
> `manager.rs:675` (`Field::new("item", DataType::Float32, true)`), so every token
> costs `dim × 4` bytes — the ~500 KB/entry footprint RFC 0086 names as the
> multivector cost. Storage **is** the late-interaction cost (one vector per *token*,
> not per *doc*), so the inner precision is the lever. Make it a per-namespace choice
> (`f32` default, `f16`) fixed at first write like dimension/kind/metric, and adopt
> **[Iso-ModernColBERT](https://huggingface.co/topk-io/Iso-ModernColBERT)** (128-dim,
> cosine, f16, Apache-2.0) as the reference model the BEIR multivector harness
> benchmarks against. Storage is an **engine** concern (`CLAUDE.md` § "search owns
> the engine"), so the fix is here. Hard fork: lands here, stays here, no upstream PR.

## Summary

Add a per-namespace **token-vector precision** for multivector namespaces — `f32`
(today's behavior, the default) and `f16` — fixed at the namespace's first write
alongside vector kind, dimension, row-id type, and distance metric. `f16` halves
the dominant late-interaction cost (token-bag storage and the bytes the foyer cache
and object reads move) at the precision the reference model already emits, riding
LanceDB's existing half-precision vector support rather than new ANN code. Adopt
**Iso-ModernColBERT** as the engine's reference late-interaction model — 128-dim,
cosine, f16 — which matches the multivector path's forced-cosine constraint
(`manager.rs:221`, `:252-258`) exactly, and re-run the BEIR multivector harness
(today's baseline is `lightonai/LateOn` at f32) to quantify the recall / footprint /
latency trade. Name **int8 / 4-bit residual (PLAID-style)** compression as the next
rung — bigger savings, needs a reconstruct-and-rerank step — and the **pooled-FDE
two-stage** retrieval path as an explicitly-deferred architectural choice, not v1.

## Background: late interaction works, but every token costs four bytes

The multivector path is built and benchmarked. A namespace whose first write sets
the `vectors` field (`manager.rs:1220`) is a `VectorKind::Multivector` namespace:

```rust
// crates/hevsearch-core/src/manager.rs:675-680 (schema_for_kind)
let inner_item = Arc::new(Field::new("item", DataType::Float32, true));
let vector_type = match kind {
    VectorKind::Single => DataType::FixedSizeList(inner_item, dim as i32),
    VectorKind::Multivector => {
        let inner_fsl = DataType::FixedSizeList(inner_item, dim as i32);
        DataType::List(Arc::new(Field::new("item", inner_fsl, true)))
    }
};
```

- The bag is `List<FixedSizeList<Float32, dim>>` — variable-length, one
  `FixedSizeList` per token (`vector.rs:6`, `:26`). The inner item is **always
  `Float32`**; there is no precision option.
- Query construction passes the first query sub-vector to `.nearest_to(...)` and
  pushes the rest via `.add_query_vector(...)`, which LanceDB detects as a
  late-interaction (MaxSim) plan (`manager.rs:1664-1693`).
- The metric is **forced to cosine** for multivector — `default_for_kind`
  (`manager.rs:221`) and `validate_for_kind` rejects anything else
  (`manager.rs:253-258`).
- Schema introspection round-trips the shape: `List<FixedSizeList<Float32, dim>>`
  → `VectorKind::Multivector` (`manager.rs:2483-2485`).

The cost is structural. A single-vector doc stores one `dim × 4`-byte vector; a
multivector doc stores `T × dim × 4` bytes for `T` tokens. At `dim = 128`, that is
**512 bytes per token** — and RFC 0086's "~500 KB/entry" implies token counts in
the high hundreds to ~1000 per entry. The benches confirm late interaction is the
heavy path (seconds-scale p50 cold, `bench/results/beir_multivector_objcache.md`),
and the bytes the foyer cache (L1 RAM / L2 NVMe) and object reads move scale with
that footprint. **Storage is the late-interaction cost; precision is the lever that
does not touch the model or the query algebra.**

## Why f16, and why ride LanceDB

- **The reference model already emits f16.** Iso-ModernColBERT's documented storage
  format is `token_embeddings: emb.astype(np.float16)` — f16 is the model's native
  serving precision, recovering f32-level BEIR quality (its card reports an
  **NDCG@10 of 53.44** in bf16). Storing f32 in the engine doesn't buy accuracy the
  model produced; it pays 2× for precision the embedder already discarded.
- **It is a one-field change at a known site.** The hardcoded `DataType::Float32` at
  `manager.rs:675` becomes a per-namespace choice. Single-vector namespaces already
  prove the pattern (kind, dim, id type, metric are all fixed-at-first-write,
  immutable thereafter, validated on every subsequent write — `manager.rs:932-948`).
  Precision is one more such property.
- **LanceDB ships half-precision vectors on the pin.** `lancedb = "=0.29.0"`
  (`Cargo.toml:24`) supports `Float16` fixed-size-list vector columns; the additive
  work is to construct the inner item as `Float16` when the namespace selects it,
  not to write a quantizer. (The load-bearing confirmation — that LanceDB's
  *late-interaction* MaxSim plan accepts an `f16` inner item, vs. only the
  single-vector path — is an open question below, with a no-regret fallback.)
- **128-dim cosine is the model's shape and the engine's only multivector shape.**
  No metric negotiation: Iso-ModernColBERT is cosine, multivector is cosine-only,
  they coincide. Dimension 128 is the model's per-token dim and a comfortable
  `FixedSizeList` width.

## Design

### Per-namespace token-vector precision, fixed at first write

Add `token_precision ∈ {f32, f16}` to the multivector namespace shape, defaulting
to `f32` so nothing changes on upgrade. Like dimension and metric, it is **inferred
or set on the first write and immutable thereafter**; subsequent writes that carry a
different precision are rejected with the same fail-fast validation as a dimension
or metric mismatch (`manager.rs:932-948`). It is **multivector-only** in v1 —
single-vector f16 is a separate, easy follow-up but is not the footprint problem
this RFC exists to solve.

The choice is encoded in the Arrow schema (the inner item's `DataType`), so it
round-trips through `schema_for_kind` (`manager.rs:667-680`) and the introspection
path (`manager.rs:2480-2508`) without a side-channel — read it back from the table
schema, never persist it twice where it can drift (the RFC 0009 discipline).

### What changes per site

1. **Schema construction** (`manager.rs:675`): build the inner item as `Float16` or
   `Float32` per the namespace precision instead of the hardcoded `Float32`. The
   `Single`/`Multivector` branch structure (`:677-680`) is unchanged.
2. **Write path** (`manager.rs:870-1040`, `upsert`): cast incoming token sub-vectors
   to the namespace precision (down-cast f32→f16 on the way in if the caller sends
   f32; accept f16 directly). Infer precision on first write; validate it matches on
   every subsequent write, beside the existing kind/dim/metric checks.
3. **Introspection** (`manager.rs:2480-2508`): classify `List<FixedSizeList<Float16,
   dim>>` as `Multivector` exactly as the `Float32` form is classified today; surface
   the precision in `GET /ns/{ns}` so callers (and Layer's operator) can read which
   precision a namespace carries.
4. **Query path** (`manager.rs:1664-1693`): the MaxSim plan is unchanged in shape;
   the query sub-vectors are cast to the stored precision before
   `.nearest_to(...)` / `.add_query_vector(...)` if LanceDB requires matching
   precision (confirm on the pin — open question).
5. **Request/response types** (`query.rs`, `result.rs`): `token_precision` on the
   namespace-creating request surface; reported on namespace info. Query results for
   multivector hits already return `vector: None` to save bytes (the agent-confirmed
   behavior), so the response surface is unaffected.
6. **Wire** (`docs/api.html`): document `token_precision`, its default, its
   immutability, and the recommendation to send f16 token bags for an f16 namespace.

### Iso-ModernColBERT as the reference model

The engine is bring-your-own-vector (`CLAUDE.md` § "Embedding"), so adopting a
reference model is a **benchmark and documentation** act, not a code dependency —
the engine never runs the model. But naming one matters: it fixes the dimension
(128), the metric (cosine), and the precision (f16) the storage path is tuned for,
and it gives the BEIR harness a single, reproducible, Apache-2.0 embedder to report
against instead of the incumbent `lightonai/LateOn`. The edge twin (Layer RFC 0089)
is where the model is actually *run* — doc-side and query-side embedding is Layer's
job, not the engine's.

### Compression beyond f16 — named, not shipped

`f16` is the free rung (the model's own precision, a one-field change, LanceDB
native). Below it:

- **int8 / scalar-quantized token vectors** (~4× vs f32) and **4-bit residual
  (PLAID-style)** (~8–16×) cut far deeper, but they are not a precision toggle —
  they need a centroid/codebook and a **reconstruct-and-rerank** step (decode
  candidates against full precision before final MaxSim) to hold recall. That is a
  quantizer and a two-phase scorer, its own RFC, gated on whether f16 alone closes
  enough of the 500 KB/entry gap. Named here so the ceiling is visible; deferred so
  v1 stays "ride what LanceDB ships."

## Edge mapping (how Layer uses this)

The edge twin is **Layer RFC 0089** (late interaction at the edge), which answers
the "multivector has no portable-subset spelling — a later question" that RFC 0086
§ Non-goals parked. The split is the usual one:

- **Precision is an engine concern Layer declares, not a wire knob.** Like
  `distanceMetric` (RFC 0006) and the index kind (RFC 0009), `token_precision` is
  set per namespace at creation. It rides Layer's operator `Index`/`VectorStore` CR
  surface (an operator declares an `f16` multivector namespace), not the inbound
  Turbopuffer-shaped query wire — Turbopuffer has no such knob, and the namespace is
  new-multivector-only.
- **The engine does MaxSim natively, so Layer needs no gateway-side rerank for a
  `kind: search` namespace.** This is the key difference from RFC 0049 (late
  interaction over *Turbopuffer*, which has no native MaxSim and so does two-stage
  FDE-ANN + gateway-side MaxSim). On hev search the bag is stored and scored in the
  engine; Layer maps `["vectors", "ANN", [[…]]]` straight onto the engine's
  `vectors`/MaxSim path. RFC 0089 draws the open/pro line over that surface.
- **Footprint is the cost this RFC reduces and RFC 0089 meters.** 0089 names the
  20–100× storage asymmetry as the basis for tiering managed late interaction; this
  RFC halves the engine-side half of that bill.

## Open questions (for the implementation PR)

- **Does LanceDB's MaxSim plan accept an `f16` inner item on the pin?** Single-vector
  f16 is supported; confirm the late-interaction path (`add_query_vector` loop)
  scores an `f16` `List<FixedSizeList>` column. **No-regret fallback:** if MaxSim is
  f32-internal, store the bag as `f16` and widen to `f32` at scoring time — storage
  (the dominant cost: cache bytes, object reads, S3 footprint) still halves; only
  the in-RAM working copy is f32. Quantify the widen cost on the harness.
- **Query-side precision.** Require f16 query token bags for an f16 namespace, or
  accept f32 and down-cast? Lean accept-and-down-cast (callers shouldn't need to know
  storage precision), matching how the write path absorbs precision.
- **f16 recall delta vs f32 on this engine's IVF_PQ substrate.** The model card
  reports f16 recovers f32 BEIR quality at the *model* layer; confirm the engine's
  IVF_PQ-over-tokens index doesn't compound f16 with PQ loss past a recall floor.
  Sweep on the harness before recommending f16 as the default for a quality-led
  namespace.
- **Single-vector f16.** In scope as a trivial extension, or held to keep this RFC
  multivector-only? Lean hold — single-vector storage is not the 500 KB/entry
  problem, and bundling it widens the blast radius.
- **Reporting precision.** Read it back from the LanceDB column type (can't drift)
  vs. persist in namespace metadata; prefer reading it back (RFC 0009 discipline).

## Testing

- **Integration** (`crates/hevsearch-api/tests/`, MinIO-gated, mirror
  `api_multivector.rs`): create an `f16` multivector namespace; assert
  `GET /ns/{ns}` reports `token_precision = f16` and `kind = multivector`; upsert
  f32 and f16 token bags and assert both land (down-cast on the f32 path); assert a
  MaxSim query returns `k` hits with finite scores; assert a second write with
  mismatched precision → `400` (the kind/dim/metric fail-fast shape); assert the
  default path (precision omitted) stays `f32` and every existing multivector test
  stays green.
- **Round-trip** (`manager_multivector.rs`): a write-then-introspect cycle classifies
  `List<FixedSizeList<Float16, dim>>` as `Multivector` with the right dim.
- **Cache** (`api_semantic_cache.rs` neighbors): the result-cache key is unaffected
  (precision is a namespace property, not a per-query input), so an f16 namespace's
  cache behaves identically — assert no cross-precision key collision can arise
  (distinct namespaces, distinct prefixes).

## Benchmark plan

Reuse the BEIR multivector harness and result layout
(`bench/results/beir_multivector_objcache.md`, `cold_vs_warm.md`). The headline is
**f16 vs f32 at equal model**, with the model switched to Iso-ModernColBERT:

- **Quality:** `ndcg@10` / `recall@100` for `{f32, f16}` token storage on the BEIR
  multivector slice, embedded by Iso-ModernColBERT. Pass condition: f16 holds f32
  recall within a small, named delta (the model card's f16≈f32 claim, verified on
  *this* engine's index, not just the model).
- **Footprint:** on-disk index + token-bag bytes per entry and per namespace for
  `{f32, f16}` — the ~500 KB/entry → ~250 KB/entry claim, measured, plus the foyer
  cache occupancy delta (the operational win is fewer bytes resident).
- **Latency:** cold (object-storage-resident) and warm (foyer-resident) p50/p95 for
  `{f32, f16}` — half the bytes should move faster cold; quantify, and measure the
  widen-to-f32-at-scoring cost if the fallback path is taken.
- **Baseline continuity:** re-run the incumbent `lightonai/LateOn` f32 numbers
  alongside, so the model switch and the precision switch are separable in the
  tables. Log any dataset where f16 loses — silent omission reads as "f16 is free"
  when on some slice it may not be.

f16 ships as the *recommended* (not forced) multivector precision only if the
benchmark shows the recall delta is within the named floor; otherwise it ships as an
available-but-opt-in knob and f32 stays the default.

## Alternatives considered

- **Do nothing — keep f32.** Leaves the 500 KB/entry footprint RFC 0086 flags as
  *the* multivector cost, paying 2× for precision the reference model already
  discarded to f16. Rejected.
- **Jump straight to int8 / PLAID.** Bigger savings, but it is a quantizer plus a
  reconstruct-and-rerank scorer — its own RFC. Doing it first skips the free,
  native, model-aligned f16 rung and couples the footprint win to a much larger
  change. Deferred, named as the next rung.
- **Pooled-FDE two-stage retrieval (the RFC 0049 / MUVERA shape) in the engine.**
  Add a single pooled fixed-dimensional vector per entry as the ANN-indexed column,
  with the token bag as a non-indexed payload, and rerank top-`K'` with MaxSim — the
  shape RFC 0049 uses *because Turbopuffer has no native MaxSim*. On this engine
  MaxSim **is** native (`manager.rs:1664-1693`), so two-stage is a latency/recall
  optimization (cheaper first-stage candidate generation), not a capability we lack.
  It adds a second indexed column, an FDE construction, and a `K'` knob — a genuine
  architectural choice with its own recall@K' eval, deliberately **not** bundled into
  a storage-precision RFC. Framed as a separate decision (the SPFresh treatment of
  RFC 0009): real, interesting, scoped elsewhere.
- **Per-query precision.** Rejected — precision is baked into the stored column, like
  the index kind (RFC 0009) and metric (RFC 0006). Per-namespace, fixed at first
  write, matches the engine's immutable-shape model.

## Fork delta

Pure **additive engine capability** on a hard fork — no upstream PR (`AGENTS.md`
§ "This is a hard fork"). Record the `token_precision` namespace property, the
per-precision inner-item construction at `manager.rs:675`, and the write/introspect
plumbing so a hand cherry-pick doesn't fight them. No subtractive edge removal. No
new dependency — `f16` vectors are in the pinned `lancedb 0.29.0` (`Cargo.toml:24`);
int8/PLAID, if pursued later, is the rung that would add one.

## References

- `crates/hevsearch-core/src/vector.rs:4-30` — `VectorKind::Multivector`, the
  `List<FixedSizeList<Float32, dim>>` shape, the ColBERT/MaxSim note.
- `crates/hevsearch-core/src/manager.rs:675` — the hardcoded `Float32` inner item
  (the hinge); `:677-680` (`schema_for_kind` kind branch); `:221`
  (`default_for_kind` → cosine for multivector); `:253-258` (`validate_for_kind`
  cosine-only); `:870-1040` (`upsert`, first-write shape fixing + per-write
  validation); `:932-948` (kind/dim/metric fail-fast); `:1220` (`vectors` →
  multivector); `:1664-1693` (the MaxSim `nearest_to` + `add_query_vector` plan);
  `:2480-2508` (schema → `VectorKind` introspection).
- `crates/hevsearch-core/src/query.rs`, `result.rs` — the request/response surface
  to carry `token_precision` and report it on namespace info.
- `Cargo.toml:24-25` — `lancedb = "=0.29.0"` / `lance = "=6.0.0"`; the pin that ships
  `Float16` vector columns (no bump for f16).
- `bench/results/beir_multivector_objcache.md`, `cold_vs_warm.md` — the report
  tables this extends; incumbent baseline is `lightonai/LateOn` at f32.
- [Iso-ModernColBERT](https://huggingface.co/topk-io/Iso-ModernColBERT) — the
  reference model: ModernBERT-base (~0.1B), 128-dim per-token, cosine/MaxSim, f16
  serving precision, Apache-2.0; BEIR NDCG@10 53.44 (bf16).
- `docs/rfcs/0006-configurable-distance-metric.md` — the per-namespace
  fixed-at-creation precedent; `docs/rfcs/0009-pluggable-vector-index-type.md` — the
  `refine_factor`/HNSW work that explicitly scoped multivector out (this RFC is its
  multivector-side companion).
- `../layer/docs/rfcs/0089-late-interaction-edge-surface.md` — the edge twin (the
  MaxSim query surface, the managed embedder, the open/pro line);
  `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` § Non-goals — the
  "~500 KB/entry … a later question" this RFC takes up;
  `../layer/docs/rfcs/0049-late-interaction-video-retrieval.md` — the two-stage
  FDE+gateway-MaxSim shape for a store *without* native MaxSim (contrast).
- `CLAUDE.md` § "search owns the engine" / "Embedding" (BYO-vector);
  `AGENTS.md` § "This is a hard fork".
