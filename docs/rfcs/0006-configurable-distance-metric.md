# RFC 0006: Configurable distance metric

Tracking issue: _TBD_

> **Status:** draft, proposal. **Additive engine capability.** The vector distance
> metric is **hardcoded by vector kind** — L2 for single-vector, cosine for
> multivector — with no per-namespace override (`manager.rs:1477-1481`; the inline
> comment says so outright: "The API surface does not expose a metric override").
> Turbopuffer namespaces, by contrast, declare a `distance_metric`
> (cosine / euclidean / dot), and Layer fronts this engine with that wire
> (`../layer/docs/rfcs/0086-…`, the edge twin). A cosine- or dot-product namespace
> therefore cannot be mirrored onto `search` faithfully today. The metric is an
> index/scoring property — **engine** side of the split (`CLAUDE.md`) — so the fix
> is here. Hard fork: lands here, stays here, no upstream PR.

## Summary

Make the distance metric a **per-namespace property, fixed at namespace creation**
(the same first-write-fixes-the-shape rule the engine already uses for vector kind
and dimension), chosen from the metrics LanceDB supports — `l2` (Euclidean),
`cosine`, and `dot` — with the current behavior as the default. This turns Layer's
`distanceMetric` validation from a check with nothing to check into a real
contract, and lets a cosine/dot namespace land on the engine without edge-side
vector normalization.

## Background: the metric is fixed by kind, not chosen

```rust
// crates/hevsearch-core/src/manager.rs:1470-1481 (create_index)
// Single-vector namespaces use the historical L2 default;
// multivector namespaces use cosine — Lance's late-interaction
// index only supports cosine. The API surface does not expose
// a metric override on ...
let metric = match info.kind {
    VectorKind::Single      => DistanceType::L2,
    VectorKind::Multivector => DistanceType::Cosine,
};
let mut builder = IvfPqIndexBuilder::default().distance_type(metric);
```

- `DistanceType` is imported from lance (`manager.rs:60`); lance supports `L2`,
  `Cosine`, and `Dot`. The engine simply never plumbs a choice through.
- `README.md:336` states it plainly: "Cosine only" for multivector; "hev search
  does not expose a per-request metric option on the API surface; `create_index`
  constructs the IVF_PQ builder with cosine internally for multivector namespaces
  and with L2 for single-vector namespaces."
- The metric is used at **index build** (above) and must match the metric used at
  **query** time (the IVF_PQ search and the un-indexed brute-force scan both score
  by a distance); today both are implicitly L2/cosine-by-kind, so they agree by
  construction. Any configurable metric must keep build-time and query-time in
  lockstep — the same desync hazard RFC 0001 calls out for tokenization.

## Why not normalize in the gateway

cosine distance equals L2 on L2-normalized vectors, so Layer *could* normalize
inbound vectors and read cosine results off an L2 namespace. Rejected as the
primary fix:

- It mutates stored vectors, so `include_vector` returns the normalized vector, not
  the caller's — a silent fidelity break on a transparent proxy.
- It only fakes cosine; **dot product** has no L2-normalization trick (it depends on
  magnitude), so it cannot be emulated at all.
- It is edge math standing in for an engine property — the "don't make Layer
  reimplement what the engine owns" smell (`CLAUDE.md`). The metric belongs to the
  index.

So the engine exposes the metric. Layer stays the edge.

## Design

### Metric fixed per namespace at creation

Add `distance_metric ∈ { l2, cosine, dot }` to the namespace's fixed shape,
alongside vector kind and dimension:

- Settable at namespace creation — on the first upsert/import of a fresh namespace
  (carried in the request, or defaulted), and **immutable** thereafter, because the
  built index encodes it. Like vector kind, a change is delete-and-recreate.
- **Defaults preserve today's behavior**: single-vector ⇒ `l2`, multivector ⇒
  `cosine`. An existing namespace (no stored metric) reads as its kind's default,
  so nothing changes on upgrade.
- **Multivector constraint stays**: Lance's late-interaction index supports cosine
  only, so a multivector namespace rejects a non-cosine metric with `400`
  (documented, not silently coerced). The configurable axis is real only for
  single-vector namespaces in v1 — but the field is uniform.
- Stored in namespace metadata and reported by `GET /ns/{ns}` (next to
  `kind`/`vector_dim`) so callers and the operator can read it.

### What changes per site

1. **Index build** (`manager.rs:1481`): `distance_type(metric)` reads the
   namespace's stored metric instead of the kind match.
2. **Query** (the single-vector search + brute-force scan paths,
   `manager.rs:1388`/around the `nearest` builder): score by the same stored
   metric, so indexed and un-indexed queries agree with the build.
3. **Namespace metadata / `resolve_schema_info`** (`manager.rs:746`): carry and
   persist `distance_metric`; surface it in the `/ns/{ns}` info response.
4. **Validation**: reject a metric change against a fixed namespace (`400`); reject
   non-cosine on multivector (`400`).
5. **Wire** (`docs/api.html`): document `distance_metric` on namespace creation and
   in the `/ns/{ns}` response; note the immutability and the multivector
   constraint.

### Semantic-cache interaction

The semantic cache's `min_similarity` is a **cosine** floor by definition
(`README.md` semantic-cache section). On a non-cosine namespace, decide whether
semantic caching is rejected (`400`, like other unsupported shapes) or interpreted
against the namespace metric. Simplest v1: the semantic cache stays cosine-only and
returns the existing `unsupported_query_shape` rejection on non-cosine namespaces.

## Edge mapping (how Layer uses this)

For the edge twin (RFC 0086): Layer's `Index.spec.backend.distanceMetric` maps to
the engine's per-namespace `distance_metric`. The operator's existing
`Ready=False/Reason=MetricMismatch` validation becomes a real contract — it
checks the CR's declared metric against the engine's stored metric — instead of
asserting against a fixed value. A Turbopuffer namespace declared `cosine_distance`
or `euclidean` can be recreated faithfully on `search`. Until this lands, only L2
(single-vector) namespaces mirror exactly, which is the current matrix state
(`../layer/site/src/content/docs/kubernetes/store-support.mdx`, "Distance metric:
fixed").

## Open questions (for the implementation PR)

- **Where the metric is fixed** — first upsert (alongside kind/dim) vs. first index
  build. First upsert is more consistent with how kind/dim are fixed; confirm the
  brute-force (pre-index) query path can honor it from row one.
- **`dot` in IVF_PQ on the pin** — confirm lance 6.0.0's IVF_PQ builder accepts
  `DistanceType::Dot` and that the brute-force scan honors it identically.
- **Multivector** — keep cosine-only (reject others) in v1, or revisit if Lance
  later widens late-interaction metrics.
- **Score sign/range** — `results[].score` is a distance for vector queries
  (`docs/api.html`); confirm dot/cosine score orientation is documented so callers
  read it correctly (smaller-is-nearer vs. larger-is-more-similar).
- **Semantic cache on non-cosine** — reject vs. reinterpret (lean reject in v1).

## Testing

- **Integration** (`crates/hevsearch-api/tests/`): create single-vector namespaces
  with `l2`, `cosine`, `dot`; assert ranking order matches the chosen metric on a
  fixture where the metrics disagree; assert indexed and brute-force (pre-index)
  results agree for each metric; multivector + non-cosine → `400`; metric change on
  a fixed namespace → `400`; default (omitted) preserves L2/cosine-by-kind and
  existing tests stay green.
- **`/ns/{ns}`** reports the stored metric.

## Alternatives considered

- **Gateway-side normalization for cosine.** Rejected — mutates returned vectors,
  cannot emulate dot product, and is edge math for an engine property (`CLAUDE.md`).
- **Per-request metric.** Rejected — the metric is baked into the built index;
  a per-request choice would force re-ranking or a second index. Per-namespace,
  fixed at creation, matches both Lance's index model and Turbopuffer's wire.
- **Do nothing.** Leaves cosine/dot Turbopuffer namespaces unmirrorable on the
  owned engine and the operator's `distanceMetric` validation hollow. Rejected.

## Fork delta

Pure **additive engine capability** on a hard fork — no upstream PR (`AGENTS.md`
§ "This is a hard fork"). Record the `distance_metric` namespace property and the
build/query plumbing so a hand cherry-pick doesn't fight it. No subtractive edge
removal.

## References

- `crates/hevsearch-core/src/manager.rs:1470-1481` — the hardcoded metric-by-kind
  and the "no override" comment; `:1447` (`create_index`); `:60` (`DistanceType`
  import — L2/Cosine/Dot available); `:1388` (query path that must match);
  `:746` (`resolve_schema_info`, where namespace shape is carried).
- `README.md:336` — "Cosine only" multivector / no per-request metric option.
- `docs/api.html` — `results[].score` is a distance; `/ns/{ns}` info shape to
  extend; semantic-cache cosine floor.
- `Cargo.toml:24-25` — `lancedb = "=0.29.0"` / `lance = "=6.0.0"` (the pin whose
  IVF_PQ `Dot` support the PR confirms).
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — edge twin;
  `Index.spec.backend.distanceMetric` and the `MetricMismatch` validation this
  makes real.
- `CLAUDE.md` § "Engine (keep) vs edge (shed)" / "What the engine is NOT";
  `AGENTS.md` § "The engine/edge test".
