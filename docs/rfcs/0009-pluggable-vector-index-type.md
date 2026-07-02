# RFC 0009: Pluggable vector index type (HNSW)

Tracking issue: _TBD_

> **Status:** draft, proposal. **Additive engine capability.** The vector index is
> **hardcoded to IVF_PQ**. `create_index` always constructs an `IvfPqIndexBuilder`
> and builds `Index::IvfPq` (`manager.rs:1789-1801`); the request already carries a
> `kind` field but the handler rejects everything except `"ivf_pq"`
> (`handlers.rs:390`, `query.rs:276`). The query path exposes a single ANN knob —
> `.nprobes(nprobes)` (`manager.rs:1697`) — with no candidate re-ranking. The index
> structure is an **engine** concern (`CLAUDE.md` § "search owns the engine"), so
> the fix is here. Hard fork: lands here, stays here, no upstream PR.

## Summary

Turn the existing-but-inert `IndexRequest.kind` field into a real selector over the
vector index types LanceDB already ships in the pinned dependency — starting with
the **IVF_HNSW family** (`ivf_hnsw_sq`, `ivf_hnsw_pq`) alongside today's `ivf_pq` —
and expose two query-time knobs LanceDB already supports but the engine never
plumbs: `ef` (HNSW search depth) and `refine_factor` (re-rank quantized candidates
against full-precision vectors). The index kind and its build params are fixed per
namespace at build time, with `ivf_pq` as the default so nothing changes on
upgrade. This is a **capability-exposure** RFC, not a new ANN implementation: the
graph index, its builders, and the search knobs are all in `lancedb 0.29.0`.

## Background: one index type, one search knob

```rust
// crates/hevsearch-core/src/manager.rs:1789-1801 (create_index)
let mut builder =
    IvfPqIndexBuilder::default().distance_type(info.distance_metric.to_lance());
if let Some(n) = num_partitions  { builder = builder.num_partitions(n); }
if let Some(m) = num_sub_vectors { builder = builder.num_sub_vectors(m); }
if let Some(b) = num_bits        { builder = builder.num_bits(b); }

tbl.create_index(&["vector"], Index::IvfPq(builder))
    .execute()
    .await
```

- The HTTP handler hard-rejects any other kind (`handlers.rs:390`):
  `if req.kind != "ivf_pq" { ... "only \"ivf_pq\" is supported" }`. `IndexRequest`
  (`query.rs:276`) defaults `kind` to `"ivf_pq"` (`default_index_kind`,
  `query.rs:296`) and otherwise only carries PQ knobs (`num_partitions`,
  `num_sub_vectors`, `num_bits`).
- The query path sets exactly one ANN parameter — `.nprobes(nprobes)`
  (`manager.rs:1697`, default `DEFAULT_NPROBES = 20`, `query.rs`). There is **no**
  `refine_factor` and **no** `ef`, so PQ candidates are never re-scored against
  their full-precision vectors.
- The engine is already *aware* of the graph index types: `classify_index_types`
  (`manager.rs:2322`) is exhaustive over LanceDB's `IndexType` and already maps
  `IvfHnswPq` / `IvfHnswSq` / `IvfHnswFlat` to "vector" (`manager.rs:2328-2334`).
  The classification side is done; only the **build** and **query** sides are
  hardcoded.

So the gap is narrow and additive: select the builder, validate per-kind params,
and thread two query knobs that LanceDB already accepts.

## Why HNSW, and why ride LanceDB

The pinned `lancedb 0.29.0` (`Cargo.toml:24`) already exports the builders and the
search knobs — no version bump, no new ANN code:

- `IvfHnswPqIndexBuilder`, `IvfHnswSqIndexBuilder`, `IvfHnswFlatIndexBuilder` in
  `lancedb::index::vector` (defaults `m = 20`, `ef_construction = 300`); `Index`
  has the matching `IvfHnswPq` / `IvfHnswSq` / `IvfHnswFlat` variants.
- The `VectorQuery` builder already has `ef(usize)` (HNSW search depth) and
  `refine_factor(u32)` (re-rank candidates with stored vectors), next to the
  `nprobes` we already use.

**IVF_HNSW, not a global HNSW graph, is the right shape for this engine.** A single
flat HNSW graph wants the whole graph resident in RAM — which fights the engine's
moat: cold data lives on object storage, the foyer (L1 RAM / L2 NVMe) caches the
hot working set (`CLAUDE.md` § "search owns the engine"). LanceDB's IVF_HNSW builds
a navigable graph *inside each IVF partition*, so the partition-as-object storage
model is preserved and only probed partitions' graphs are pulled and cached. It is
a graph index that fits the substrate we already have.

**`refine_factor` attacks a real recall ceiling, independent of index kind.** The
existing benches show PQ recall is quantization-bound, not search-effort-bound: in
`bench/results/beir_multivector_raw/nprobes_sweep_fiqa/`, `recall@100` is flat at
**0.707** across `nprobes ∈ {8, 20, 50, 100}`, while
`index_config_sweep_fiqa/` shows recall moving with PQ width
(`sub32_8bit` 0.714 vs `sub32_4bit` 0.693). With no refine step, raising `nprobes`
buys latency, not recall. Plumbing `refine_factor` lets a lossy-but-cheap index
(PQ, SQ) recover recall by re-scoring the top candidates against full vectors —
useful for `ivf_pq` *today* and for `ivf_hnsw_sq`/`_pq` tomorrow.

## Design

### Index kinds exposed in v1

| `kind` | Builder | Compression | Notes |
|---|---|---|---|
| `ivf_pq` (default) | `IvfPqIndexBuilder` | PQ | today's behavior, unchanged |
| `ivf_hnsw_sq` | `IvfHnswSqIndexBuilder` | SQ (8-bit, 4×) | per-partition HNSW graph; SQ is the balanced first graph index |
| `ivf_hnsw_pq` | `IvfHnswPqIndexBuilder` | PQ | per-partition HNSW graph over PQ codes; smallest footprint |

`ivf_hnsw_flat` (raw vectors, highest recall, highest memory/disk) and the
non-graph `ivf_flat` / `ivf_sq` / `ivf_rq` are **named, not shipped** in v1 — easy
follow-ups once the dispatch exists, but the SQ/PQ graph pair is enough to answer
"does graph-based help our object-storage workload."

### Index kind fixed per namespace at build time

Like today's index, the kind is chosen on the `POST /ns/{ns}/index` build and
encoded in the built artifact. Rebuilding with a different `kind` replaces the
index (same evict-and-rebuild path `create_index` already uses,
`manager.rs:1810`). The kind is **not** a per-query choice — the graph is baked in.
`GET /ns/{ns}` already reports `has_vector_index` (`result.rs`); extend it to also
report the **built kind** so callers and the operator can read which index a
namespace carries.

### Per-kind build params on `IndexRequest`

Extend `IndexRequest` (`query.rs:276`) so each kind's params are expressible; all
optional, all defaulting to LanceDB's defaults:

- shared IVF: `num_partitions`
- HNSW (graph kinds): `m` (default 20), `ef_construction` (default 300)
- PQ (`ivf_pq`, `ivf_hnsw_pq`): `num_sub_vectors`, `num_bits` (4 or 8)
- SQ (`ivf_hnsw_sq`): none today (LanceDB fixes SQ at 8-bit)

A param that doesn't apply to the chosen kind is rejected with `400` (e.g.
`num_bits` against `ivf_hnsw_sq`), the same fail-fast posture as
`validate_ivf_pq_options` (`query.rs:315`).

### Query-time knobs: `ef` and `refine_factor`

Add both to `QueryRequest` and thread them into the vector query
(`manager.rs:1695-1698`), next to `nprobes`:

- `ef` — HNSW search depth; ignored (or `400`, TBD) on a non-graph index.
- `refine_factor` — re-rank the top `k × refine_factor` quantized candidates
  against full-precision vectors before returning `k`. Applies to any lossy index
  (`ivf_pq`, `ivf_hnsw_sq`, `ivf_hnsw_pq`).

Both default to unset (today's behavior). The result cache key must include them —
they change result *content*, so a query with `refine_factor` set must not collide
with one without (the same versioned-key discipline the cache already applies to
`k` / `nprobes`).

### What changes per site

1. **Handler** (`handlers.rs:390`): replace the `kind != "ivf_pq"` rejection with a
   match over the supported set; keep the synchronous pre-spawn validation.
2. **Index build** (`manager.rs:1789-1801`): `match kind` to pick the builder
   (`IvfPq` / `IvfHnswSq` / `IvfHnswPq`), apply that kind's params, wrap in the
   matching `Index::*` variant. `distance_type` plumbing (RFC 0006) is unchanged.
3. **Request types** (`query.rs:276`): per-kind build params on `IndexRequest`;
   `ef` / `refine_factor` on `QueryRequest`.
4. **Validation** (`query.rs:315`): generalize `validate_ivf_pq_options` into a
   per-kind validator (reject inapplicable params, keep the 4-bit/even-sub-vector
   rule for PQ kinds).
5. **Query** (`manager.rs:1695-1698`): thread `ef` / `refine_factor` onto the
   `VectorQuery` builder alongside `.nprobes(...)`.
6. **Namespace info** (`result.rs`, `resolve_schema_info`): surface the built index
   kind in `GET /ns/{ns}`.
7. **`classify_index_types`** (`manager.rs:2322`): already classifies the HNSW
   variants — no change, but the exhaustive match is the compile-time guard that
   keeps it honest.
8. **Wire** (`docs/api.html`): document the `kind` enum, per-kind build params, and
   the `ef` / `refine_factor` query knobs.

### Multivector interaction

Multivector namespaces are cosine-only and ride Lance's late-interaction index
(`manager.rs:252-260`). v1 keeps the index-kind selector **single-vector only**:
multivector namespaces stay on their existing path and reject a non-default `kind`
with `400` (documented, not silently coerced), the same shape as the existing
metric constraint. `refine_factor` on multivector is an open question (below). This
keeps the heavy late-interaction path (seconds-scale p50 in
`bench/results/beir_multivector_objcache.md`) out of scope for the first cut.

## Edge mapping (how Layer uses this)

For the edge twin (RFC 0086): the index kind is an engine build-time concern Layer
does not need to expose on the Turbopuffer-shaped inbound wire — Turbopuffer's API
has no index-type knob. The natural surface is the operator's `Index` CR
(`Index.spec.backend.*`), letting an operator declare `ivf_hnsw_sq` with `m` /
`ef_construction` for a namespace, mirroring how `distanceMetric` (RFC 0006) is
declared. The data-path query knobs (`ef`, `refine_factor`) can ride Layer's query
mapping if a caller needs them, but default-off keeps the wire unchanged. Until
this lands, every namespace is IVF_PQ — which is the current matrix state.

## Open questions (for the implementation PR)

- **`ef` / `refine_factor` on a non-applicable index** — reject (`400`) vs.
  silently ignore. Lean reject for `ef` on a non-graph index; lean ignore-with-no-op
  for `refine_factor` on a flat index. Confirm LanceDB's behavior on the pin.
- **`refine_factor` cost on object storage** — re-ranking re-reads full vectors;
  confirm it pulls from the object/foyer cache and quantify the latency/recall
  trade on the BEIR harness before recommending a default.
- **Default `m` / `ef_construction`** — keep LanceDB's 20 / 300, or tune for the
  S3-resident regime where graph traversal touches more byte-ranges.
- **Multivector** — keep selector single-vector-only in v1, or evaluate
  `refine_factor` against the late-interaction path (where the recall/latency
  pressure is highest).
- **Index-kind change semantics** — rebuilding with a new `kind` replaces the
  index; confirm the manifest-bump + handle-evict path (`manager.rs:1810`) handles
  a kind swap cleanly and the cache stays correct across it.
- **Reporting the built kind** — read it back from LanceDB index metadata vs.
  persist it in namespace metadata; prefer reading it back so it can't drift.

## Testing

- **Integration** (`crates/hevsearch-api/tests/`, MinIO-gated, mirror
  `api_index.rs` / `api_index_num_bits.rs`): build each kind
  (`ivf_pq`, `ivf_hnsw_sq`, `ivf_hnsw_pq`) on a single-vector namespace and assert
  `GET /ns/{ns}` reports the built kind and `has_vector_index = true`; assert a
  graph-index query with `ef` returns `k` hits; assert `refine_factor` changes
  ranked order on a fixture where PQ and full-precision disagree; assert
  inapplicable params (`num_bits` on `ivf_hnsw_sq`) → `400`; assert non-default
  `kind` on a multivector namespace → `400`; assert the default path
  (`kind` omitted) stays IVF_PQ and every existing test stays green.
- **Cache** (`api_semantic_cache.rs` neighbors): assert a `refine_factor` query and
  an otherwise-identical non-refine query do not share a cached result.

## Benchmark plan

Reuse the existing BEIR harness and result layout
(`bench/results/beir_multivector_raw/`). The headline comparison is **HNSW vs IVF
at equal recall** on single-vector datasets:

- **Quality/latency sweep**, per kind (`ivf_pq`, `ivf_hnsw_sq`, `ivf_hnsw_pq`):
  `recall@100` / `ndcg@10` vs. `qps` / `p50` / `p95`, sweeping the kind's search
  knob (`nprobes` for IVF, `ef` for HNSW) and `refine_factor ∈ {none, 2, 5, 10}`.
  This directly tests whether the graph index reaches a given recall at lower
  latency than IVF_PQ, and how much `refine_factor` lifts the flat-0.707 PQ ceiling
  documented in `nprobes_sweep_fiqa/`.
- **Cold vs. warm** (extend `cold_vs_warm.md`): graph indexes change the
  object-storage read pattern — measure cold first-query and warm (foyer-resident)
  latency per kind, since the per-partition graph touches different byte-ranges than
  a PQ scan.
- **Build cost / index size**: wall-clock build time and on-disk index bytes per
  kind — the graph + `ef_construction` make HNSW builds more expensive than PQ; the
  matrix must show the trade, not just query-side wins.

A new kind ships only if the benchmark shows it beats IVF_PQ on the
recall-at-latency or size frontier for some real dataset; otherwise it stays behind
the selector unused. Log any dataset where it loses — silent omission reads as
"HNSW always wins" when it may not.

## Alternatives considered

- **Global (flat) HNSW graph.** Rejected — wants the whole graph RAM-resident,
  which fights the object-storage + foyer model that is the engine's moat
  (`CLAUDE.md`). LanceDB's per-partition IVF_HNSW keeps the partition-as-object
  layout and is the variant actually shipped on the pin.
- **SPFresh / SPANN.** A genuinely interesting follow-up, deferred. SPFresh's
  headline is *incremental in-place index maintenance* — directly relevant to the
  engine's one-shot rebuild limitation (`create_index` retrains over all rows;
  there is no incremental add). But it is **not** in LanceDB: adopting it means FFI
  into its C++ implementation (or a reimplementation) **and** its own on-disk layout
  outside the Lance dataset substrate every other index, the storage layer, and the
  caches assume. That is a "do we outgrow LanceDB as the index substrate" decision,
  not an additive feature — its own RFC, motivated by a live-update workload this
  one does not address. This RFC deliberately scopes to what rides the existing
  dependency.
- **Per-request index kind.** Rejected — the graph is baked into the built index;
  a per-request kind would force multiple indexes or re-ranking. Per-namespace,
  fixed at build, matches LanceDB's model (and mirrors RFC 0006's metric choice).
- **Do nothing.** Leaves the engine single-index with a PQ recall ceiling no search
  knob can lift (`nprobes_sweep_fiqa/`), and leaves `ef` / `refine_factor` —
  already in the dependency — unused. Rejected.

## Fork delta

Pure **additive engine capability** on a hard fork — no upstream PR (`AGENTS.md`
§ "This is a hard fork"). Record the `kind` selector, the per-kind build params, and
the `ef` / `refine_factor` query plumbing so a hand cherry-pick doesn't fight them.
No subtractive edge removal. No new dependency — every builder and query knob is in
the pinned `lancedb 0.29.0` (`Cargo.toml:24-25`).

## References

- `crates/hevsearch-core/src/manager.rs:1789-1801` — hardcoded `IvfPqIndexBuilder` /
  `Index::IvfPq`; `:1697` (`.nprobes(...)`, the only ANN query knob); `:1810`
  (evict-and-rebuild path); `:2322-2340` (`classify_index_types`, already exhaustive
  over the HNSW variants); `:252-260` (multivector cosine-only constraint).
- `crates/hevsearch-api/src/handlers.rs:390` — the `kind != "ivf_pq"` rejection.
- `crates/hevsearch-core/src/query.rs:276` (`IndexRequest`), `:296`
  (`default_index_kind`), `:315` (`validate_ivf_pq_options`), `DEFAULT_NPROBES`.
- `crates/hevsearch-core/src/result.rs` — `NamespaceInfo` / `has_vector_index`,
  the `/ns/{ns}` response to extend with the built kind.
- `lancedb 0.29.0` — `index/vector.rs:385` (`IvfHnswPqIndexBuilder`), `:440`
  (`IvfHnswSqIndexBuilder`), `:485` (`IvfHnswFlatIndexBuilder`); `index.rs:55-72`
  (`Index::IvfHnswPq` / `IvfHnswSq`); `query.rs:1123` (`ef`), `:1155`
  (`refine_factor`), `:1041` (`nprobes`).
- `Cargo.toml:24-25` — `lancedb = "=0.29.0"` / `lance = "=6.0.0"` (the pin that
  ships the HNSW builders and the query knobs; no bump needed).
- `bench/results/beir_multivector_raw/nprobes_sweep_fiqa/` — `recall@100` flat at
  0.707 across nprobes (the PQ ceiling); `index_config_sweep_fiqa/` — recall vs. PQ
  width; `bench/results/beir_multivector_objcache.md` / `cold_vs_warm.md` — the
  report tables this extends.
- `docs/rfcs/0006-configurable-distance-metric.md` — the per-namespace
  fixed-at-creation precedent and the `distance_type` plumbing this builds on.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — edge twin; the
  `Index` CR surface an operator-declared index kind would map to.
- `CLAUDE.md` § "search owns the engine" / "Engine (keep) vs edge (shed)";
  `AGENTS.md` § "The engine/edge test" / "This is a hard fork".
