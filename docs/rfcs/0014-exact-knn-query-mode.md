# RFC 0014: Exact KNN as an explicit query mode

> **Status:** draft, proposal. Add an explicit **exact (brute-force) KNN mode**
> to `/query` — `exact: true` — that bypasses the vector index and scans every
> row, returning 100%-recall results. The implementation hook already exists in
> the pinned LanceDB: `VectorQuery::bypass_vector_index()`
> (`lancedb-0.29.0/src/query.rs:1184`); the engine just never calls it. The
> philosophical reference is [LodeDB](https://github.com/Egoist-Machines/LodeDB)'s
> exact-by-default posture; this RFC adopts exact-on-request, not
> exact-by-default.

## Summary

The engine can already scan brute-force — but only *by accident*. When a
namespace has no vector index, LanceDB "brute-force scans every row, which is
fine for tiny development corpora" (`manager.rs:1520-1524`). The moment an
IVF_PQ index is built, every query goes through it, and there is **no way to
ask for the exact answer**: the query surface exposes only `nprobes`
(`manager.rs:1561`, default 20). Exactness is a side effect of not having
built an index yet, not a capability.

This RFC makes it a capability: `exact: true` on `/query` routes the vector
(and multivector MaxSim) search through `bypass_vector_index()`, prefilter
(`only_if`) and projection behavior unchanged. `k`, `filter`,
`include_vector`, hybrid/FTS composition all keep their semantics; `nprobes`
is ignored (rejected?) when `exact` is set.

## Motivation

1. **Ground truth for recall measurement, in-engine.** RFC 0011's recall
   harness needs exact nearest neighbors to score against. For SIFT/GIST that
   ships with the dataset, but for *any other corpus* — a customer namespace, a
   BEIR multivector set, a filtered subset — ground truth must be computed
   somewhere. With `exact: true`, the harness computes it through the same
   engine, same distance metric, same filter path it is evaluating: run the
   query exact, run it approximate, diff. This makes recall self-calibrating on
   arbitrary workloads and — critically — measures **filtered recall** (RFC
   0011's selectivity axis) with ground truth that respects the same prefilter,
   which no pre-shipped dataset provides.

2. **The PQ recall ceiling needs an escape hatch.** The benches show recall@100
   flat at **0.707 across nprobes** — a quantization ceiling no `nprobes`
   increase can pierce (the motivation for RFC 0009's `refine_factor`). Until
   0009 lands (and for callers whose k is small and namespace is modest),
   `exact: true` is the correct-answer path: an exact scan over a 100k-row
   namespace is cheap; a 0.707 ceiling on it is not.

3. **Small namespaces shouldn't pay ANN error at all.** Many real namespaces
   (per-tenant demo corpora, `label` sections, dev environments) are tens of
   thousands of rows. LodeDB's exact-by-default posture is right for that
   regime — exact scan at that scale is milliseconds, and 100% recall is free.
   We don't adopt exact-by-*default* (the engine's target regime is large; a
   silent default flip at index-build time would be a semantics landmine), but
   exact-on-request lets Layer or a bench choose correctness per query.

4. **Debugging surface.** "Is this a recall problem or a filter/fusion
   problem?" is currently unanswerable without exporting vectors to an
   external tool. `exact: true` answers it with one request.

## Proposal

- **`/query` gains `exact: bool` (default `false`).** When set, the vector leg
  of the plan calls `bypass_vector_index()`; everything else (prefilter,
  hybrid RRF composition, fuzzy FTS, `include_vector`, multivector MaxSim) is
  unchanged. FTS-only queries reject `exact` (nothing to bypass) — or ignore
  it; open question.
- **`nprobes` + `exact` together is a 400** — the request contradicts itself.
- **Result parity contract:** an exact query and an approximate query over the
  same committed version differ only in the candidate set. Distance metric,
  filter semantics, and score computation are identical — this is what makes
  the exact/approx diff a recall measurement rather than an apples-to-oranges
  comparison. (LanceDB's flat scan and IVF_PQ path both honor deletion
  vectors and `only_if`, so this should hold; the harness asserts it.)
- **Caching:** exact results ride the existing result cache keyed on the
  manifest generation (`manager.rs:591-651`) — `exact` must be part of the
  cache key so exact and approximate answers for the same request never
  collide. Same for the semantic cache (or exact queries skip it; open
  question — a semantic-similarity hit returning *approximate* results for an
  *exact* request violates the contract).
- **Cost guardrail (open):** an exact scan on a 100M-row namespace is a
  self-inflicted wound. Options: none in v1 (trusted internal caller — Layer —
  behind the `NetworkPolicy`, matching the RFC 0002 posture), or a row-count
  soft cap with an explicit override.

## Interactions with other RFCs

- **RFC 0011 (recall harness).** The headline consumer: ground-truth
  generation moves in-engine, filtered-recall ground truth becomes possible,
  and the harness gains an exact-vs-approx self-check on every dataset it
  loads. The harness should also record exact-scan latency per corpus size —
  that curve *is* the "when do you need an index" guidance.
- **RFC 0009 (pluggable index / refine_factor).** Complementary: 0009 raises
  the approximate ceiling; this RFC provides the reference answer that proves
  it. `refine_factor` sweeps in the harness are scored against `exact: true`
  runs.
- **RFC 0006 (configurable distance metric).** Exact scan must honor the
  namespace's metric — it comes free (LanceDB's flat path takes the same
  `distance_type`), but the parity contract makes it explicit.
- **RFC 0010 (late interaction).** Multivector MaxSim without an index is
  already a brute-force plan (`manager.rs:1520-1524`); `exact: true` makes
  that plan requestable after an index exists, giving the BEIR multivector
  harness its MaxSim ground truth (and the f16-vs-f32 precision comparison a
  clean baseline).
- **RFC 0086 (Layer twin).** Whether/how `exact` surfaces on the
  Turbopuffer-shaped inbound wire is Layer's call (Turbopuffer has no such
  knob, so it is likely operator/bench-only initially). The engine ships the
  capability; the edge decides the exposure.

## Engine vs edge boundary

Exactness is a property of the search plan — engine, unambiguously. The only
edge question is wire exposure (above). A per-tenant "may this caller run
exact scans" policy, if ever wanted, would be Layer's authorization concern,
not an engine knob.

## Open questions

- **FTS-only + `exact`:** reject or ignore? (Hybrid with `exact` applies to
  the vector leg only — BM25 is already exact.)
- **Semantic cache:** skip on `exact: true`, or include `exact` in its key?
  Skipping is safer (the contract is exactness; similarity-matching it away is
  a category error).
- **Cost guardrail:** none (trusted-caller posture), or a soft row-count cap
  with override? If none, does `namespace_info` at least surface an
  exact-scan cost estimate?
- **Naming:** `exact: true` vs `ann: false` vs `mode: "exact"`. `exact` reads
  best on the wire and matches LodeDB/common usage.

## References

- `~/.cargo` pinned `lancedb-0.29.0/src/query.rs:1184` —
  `VectorQuery::bypass_vector_index()`; no version bump needed.
- `crates/hevsearch-core/src/manager.rs:1520-1524` — brute-force scan today
  only when no index exists; `:1541-1542,1561` — `nprobes` is the only vector
  query knob; `:591-651` — result cache keyed on manifest generation.
- `bench/results/` — recall@100 flat at 0.707 across nprobes (the PQ ceiling;
  see RFC 0009).
- RFC 0011 (ground truth + filtered recall), RFC 0009 (refine_factor),
  RFC 0010 (MaxSim baseline), RFC 0006 (metric parity) — engine neighbors.
- [LodeDB](https://github.com/Egoist-Machines/LodeDB) — exact-by-default
  reference ("Vector search: exact scan by default (100% recall); ANN is
  opt-in").
