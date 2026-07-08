# RFC 0014: Exact KNN as an explicit query mode

> **Status:** Accepted (2026-07-08). Add an explicit **exact
> (brute-force) KNN mode** to `/query` with `exact: true`. Exact mode
> bypasses the vector index, scans all committed rows that satisfy the
> query predicate, and returns the engine's 100%-recall nearest-neighbor
> answer for that query shape. The implementation hook already exists in
> the pinned LanceDB: `VectorQuery::bypass_vector_index()`. The
> philosophical reference is
> [LodeDB](https://github.com/Egoist-Machines/LodeDB)'s exact-by-default
> posture; this RFC adopts exact-on-request, not exact-by-default.

## Summary

The engine can already scan brute-force, but only as an incidental
consequence of index state. When a namespace has no vector index, LanceDB
linear-scans the table. The moment an IVF_PQ index is built, the vector query
path uses that index and the public query surface exposes only `nprobes`
(`QueryRequest::nprobes`, default 20). Exactness is a side effect of not having
built an index yet, not a requestable capability.

This RFC makes exactness a capability. `exact: true` on `/query` routes the
vector leg of a vector or hybrid query through `bypass_vector_index()`.
Prefilter (`only_if`), distance metric, projection, result shape, fuzzy FTS,
and hybrid composition keep their existing semantics. `nprobes` is invalid
when `exact` is set because there are no IVF partitions to probe.

## Motivation

1. **Ground truth for recall measurement, in-engine.** RFC 0011's recall
   harness needs exact nearest neighbors to score against. Today the harness
   can load SIFT/GIST-style `.ivecs` ground truth or compute synthetic
   brute-force ground truth in-process, but any real engine corpus, filtered
   subset, or multivector workload still needs the engine itself to answer the
   exact query it is evaluating. With `exact: true`, the harness can run the
   exact query and the indexed query through the same engine, same distance
   metric, same filter path, and compare the result sets. That is the target
   state for the bench-evidence lane.

2. **Filtered recall needs the same prefilter path as production.** RFC 0011's
   selectivity axis explicitly needs ground truth among rows that satisfy the
   predicate. Pre-shipped ANN datasets do not provide that, and external
   brute-force scripts easily drift from the engine's DataFusion/Lance filter
   behavior. Exact mode makes filtered recall an engine contract rather than a
   side calculation.

3. **The PQ recall ceiling needs an escape hatch.** The current benches show
   recall@100 flat at about 0.707 across `nprobes` on the FIQA nprobes sweep,
   a quantization ceiling that larger `nprobes` cannot pierce. RFC 0009's
   `refine_factor` work should raise the approximate ceiling; exact mode is
   the reference answer that proves it and the correctness path for modest
   namespaces where latency is acceptable.

4. **Small namespaces should not pay ANN error by default.** Many practical
   namespaces, including demo corpora, per-label slices, and development
   environments, are small enough that a scan is cheap. The engine should not
   silently flip global defaults based on size or index state, but callers and
   benches should be able to request the correct answer explicitly.

5. **Debugging surface.** "Is this a recall problem or a filter/fusion
   problem?" is currently hard to answer without exporting vectors. Exact mode
   provides a direct comparison point inside the service boundary.

## Proposal

- **`/query` gains `exact: bool` with default `false`.** Existing callers keep
  current behavior unless they opt in.
- **Vector-only and hybrid queries honor `exact: true`.** The vector leg calls
  `bypass_vector_index()` before execution. For hybrid queries, BM25/fuzzy FTS
  remains the same and exactness applies only to the vector candidate leg.
- **FTS-only queries reject `exact: true` with HTTP 400.** There is no vector
  index to bypass, and accepting a no-op flag would make request intent
  ambiguous.
- **`exact: true` and `nprobes` together reject with HTTP 400.** `nprobes` is
  an ANN/IVF query knob; exact scan has no partitions to probe. Rejecting the
  combination is clearer than silently ignoring caller input.
- **Semantic cache is disabled for exact queries.** An exact request must not
  be served by a near-duplicate cached result. Implement this as a validation
  rejection if `semantic_cache.enabled` and `exact` are both set, and ensure
  exact queries do not insert into the semantic sidecar.
- **Exact result-cache entries are allowed and keyed distinctly.** The normal
  generation-keyed result cache can cache exact results, but `exact` must be
  part of the exact-cache key so exact and indexed answers never collide.
- **Result parity contract:** an exact query and an indexed query over the same
  committed table version differ only in candidate selection. Distance metric,
  filter semantics, deletion-vector handling, projection, result decoding, and
  scoring fields use the same engine path wherever LanceDB allows it. The
  recall harness should assert this on fixture data.
- **No edge authorization is introduced here.** Exactness is an engine query
  plan. If Layer later needs per-tenant policy around exact scans, that remains
  an edge authorization concern.

## Bench-Evidence Lane

This RFC is factory evidence plumbing for RFC 0011 and the `bench-evidence`
workflow in PR #10. The current `hevsearch-bench` recall sweep establishes the
shape of the lane: it runs the real `NamespaceService`, computes or loads exact
nearest-neighbor ground truth, self-checks an unindexed linear-scan namespace,
then sweeps IVF_PQ `nprobes` and emits recall/nDCG/latency/build evidence.

After exact query mode lands, the harness should move the engine-ground-truth
case from "separate unindexed namespace or in-process brute force" to
`exact: true` against the same committed namespace being measured. That matters
for indexed namespaces, filtered recall, multivector MaxSim, and any corpus
whose exact ground truth is not shipped as `.ivecs`.

PR #10's workflow currently validates pinned mesh-account ECR digest inputs and
a corpus hash, runs `recall_sweep` against in-job MinIO, and uploads the
evidence markdown/JSON artifact. Its review notes one design flag: as written,
the job records `image_tag` as provenance while building and running the bench
from source at the checkout SHA. That is acceptable context for this RFC, but
bench publication must either run the pinned image or rename that provenance
before downstream evidence is treated as image-execution proof.

## Interactions With Other RFCs

- **RFC 0011 (recall harness).** Exact mode lets the harness generate
  ground-truth results in-engine for arbitrary corpora and filters. The harness
  should record exact-scan latency by corpus size; that curve becomes the
  practical "when do you need an index" guide.
- **RFC 0009 (pluggable index / `refine_factor`).** Complementary. RFC 0009
  raises the approximate ceiling; this RFC provides the reference answer used
  to prove it. `refine_factor` and future index-kind sweeps should score
  against `exact: true`.
- **RFC 0006 (configurable distance metric).** Exact scan must honor the
  namespace metric. The manager already applies `distance_type(...)` on the
  vector query; exact mode must preserve that call.
- **RFC 0010 (late interaction).** Multivector MaxSim without an index already
  has a brute-force behavior. Exact mode makes that behavior requestable after
  an index exists, giving the BEIR multivector harness a clean MaxSim baseline.
- **RFC 0086 (Layer twin).** Whether `exact` surfaces on Layer's
  Turbopuffer-shaped inbound wire is Layer's decision. The engine ships the
  capability; the edge decides exposure.

## Engine vs Edge Boundary

Exactness is a property of the search plan, so it belongs in the engine. The
engine should validate contradictory query knobs and execute the requested
plan. It should not add tenant policy, authorization, quota, or public exposure
rules for exact scans; those belong in Layer if they are needed.

## Resolved Questions

- **Cost guardrail (resolved 2026-07-08, hev on PR #19):** v1 ships with **no
  row-count guardrail** under the trusted-caller posture. Layer is the only
  data-path client and owns any caller-facing limits; the engine stays
  knob-free until a workload demonstrates the need.

- **Naming:** use `exact: true`. It is direct, matches common exact-search
  language, and avoids making ANN the conceptual default in the wire contract.
- **FTS-only + exact:** reject with HTTP 400. BM25 is already the relevant
  exact retrieval mode, and the flag has no vector meaning without a vector
  leg.
- **`nprobes` + exact:** reject with HTTP 400. The knobs describe mutually
  exclusive execution plans.
- **Semantic cache:** reject `semantic_cache.enabled` with exact mode and skip
  semantic sidecar insertion for exact results. Semantic-cache hits are
  approximate by design and cannot satisfy the exactness contract.
- **Exact result cache:** keep it, but include `exact` in the cache key. The
  existing exact result cache is keyed by namespace generation and canonical
  query fields; exactness must become one of those fields.

## References

- `crates/hevsearch-core/src/query.rs` — current `QueryRequest` exposes
  `nprobes`, text/fuzzy/filter/projection, and `semantic_cache`, but no
  `exact` field.
- `crates/hevsearch-core/src/manager.rs` — current query construction applies
  `distance_type(...)`, `.nprobes(...)`, `.limit(k)`, optional
  `full_text_search(...)`, `only_if(...)`, and projection before execution.
- `crates/hevsearch-core/src/service.rs` and `src/cache/semantic.rs` — exact
  result cache, semantic sidecar semantics, and cache-key shape.
- `crates/hevsearch-bench/src/bin/recall_sweep.rs` and
  `src/recall.rs` — current RFC 0011 recall harness: unindexed exact baseline,
  synthetic brute-force ground truth, `.ivecs` ground truth, and IVF_PQ
  `nprobes` sweep.
- `bench/results/beir_multivector_raw/nprobes_sweep_fiqa/` and RFC 0009 —
  recall@100 flat near 0.707 across `nprobes`, motivating an exact reference
  and future refine/index-kind work.
- RFC 0011 (recall harness), RFC 0009 (refine/index-kind work), RFC 0010
  (MaxSim baseline), RFC 0006 (metric parity), and Layer RFC 0086.
- [LodeDB](https://github.com/Egoist-Machines/LodeDB) — exact-by-default
  reference posture.
