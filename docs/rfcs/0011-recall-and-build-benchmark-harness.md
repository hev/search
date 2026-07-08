# RFC 0011: Recall + build benchmark harness

Tracking issue: [#16](https://github.com/hev/search/issues/16)

> **Status:** Accepted (2026-07-08). **Additive engine tooling + the build-memory
> knobs it surfaces.** The first in-repo recall lane now exists in
> `hevsearch-bench`: `crates/hevsearch-bench/src/recall.rs` owns the dataset
> readers, exact-NN ground truth, and `recall@k` / `ndcg@k` scoring, while
> `src/bin/recall_sweep.rs` runs a source-built `ivf_pq` + `nprobes` sweep through
> `NamespaceService`. That resolves the old "external-only recall numbers" gap for
> the initial evidence lane; filtered recall, freshness, HTTP mode, and the
> constrained build profiler remain follow-up implementation issues. Separately,
> the index **build** still passes LanceDB no memory / sampling / scratch options
> (`IvfPqIndexBuilder::default()`, `manager.rs:1790`), runs **in-process in the
> serving container** (`tokio::spawn`, not `spawn_blocking` —
> `crates/hevsearch-api/src/handlers.rs:413`), and NVMe is wired only for the foyer
> L2 (`cache/layer.rs`) and the object-cache byte-range store (`object_cache.rs`) —
> **never for build scratch.** This RFC versions the recall harness and adds a
> **constrained-RAM build axis**, because index build on container hardware is a
> guaranteed constraint, not a someday one. Engine-scoped (search/storage/indexing
> is the engine's, `CLAUDE.md` § "search owns the engine"); the store-vs-store twin
> is an *edge* artifact and lands in `../layer` (§ Edge mapping). Hard fork: lands
> here, stays here.

## Summary

Build and evolve a versioned, in-repo **recall + build** benchmark harness in
`hevsearch-bench`, first as an in-process source-built evidence lane and then,
where fidelity requires it, against the engine's REST surface. The harness runs
over seeded synthetic vectors for CI and **public ground-truth ANN datasets**
(SIFT1M/10M, GIST1M, optionally Deep) for publication-grade runs, and structures
its outputs around the four index requirements we actually care about —
**filtering, updating, recall, operational simplicity**. The harness is the
**decision instrument** for the still-open vector-index choice (the harness exists
to answer "IVF_PQ vs IVF_HNSW vs +refine, and at what build cost," not to ratify a
pre-made call) and the **validation gate** for the build-memory knobs the
constrained-build axis surfaces.

Two dimensions most ANN benchmarks skip are first-class here because they map to
two of the four requirements: **filtered recall at varying selectivity**
(requirement: filtering) and **build under a fixed cgroup RAM limit**
(requirement: operational simplicity). The full index-variant sweep depends on
RFC 0009 (the `kind` selector and the `ef` / `refine_factor` query knobs); the
accepted v1 harness is useful for `ivf_pq` + `nprobes` on day one and widens as
0009 lands.

PR #10 wraps the accepted v1 lane in `bench-evidence` CI: it validates immutable
`image_tag` / `corpus_hash` inputs, starts MinIO, runs
`cargo run --release -p hevsearch-bench --bin recall_sweep`, and uploads
`bench-evidence.md`, `bench-evidence.json`, and raw `recall_sweep_*.json`
artifacts. As written, that workflow records the pinned image digest as
provenance but runs the harness from the checked-out source, not from the image.
No published or consumed evidence artifact should imply otherwise until the job
either runs the pinned image or renames that provenance field to source/toolchain
identity.

The anonymized **store-vs-store** comparison against Turbopuffer is the *edge twin*
— a Layer-side harness (a `../layer` RFC) that **reuses this harness's dataset
loaders, ground-truth, and metric collectors**, kept internal / backend-anonymized
per RFC 0086 § Posture and Turbopuffer TOS §2.4. It is not built here.

## Background: recall v1 exists, and build is still uninstrumented

Two gaps, both grounded:

1. **Recall was unversioned; the v1 lane fixes the core scoring gap.** The original
   Rust harness only timed queries and counted cache hits / S3 bytes; it never
   loaded a dataset, held ground truth, or computed `recall@k`. Quality numbers
   existed only as JSON checked into `bench/results/beir_multivector_raw/quality_0.9.2/`
   (see that dir's `README.md`), emitted by an external tool. Current `main` now
   has in-repo readers for `.fvecs` / `.bvecs` / `.ivecs`, exact L2 ground truth,
   scorer unit tests, deterministic synthetic data, and a `recall_sweep` binary
   that self-checks a linear-scan baseline before recording `ivf_pq` sweep results.
   The remaining recall gaps are axes, not fundamentals: filtered recall,
   freshness, HTTP mode, and larger public datasets.
2. **Build is a black box with no controls.** `create_index` builds a default
   `IvfPqIndexBuilder` with only `num_partitions` / `num_sub_vectors` / `num_bits`
   (`manager.rs:1789-1801`); there is no build-memory budget, no sample-rate knob,
   and no scratch-dir control. The build is a `tokio::spawn` on the serving service,
   so its peak RSS competes with query serving and the foyer cache in one cgroup —
   on a constrained container a large build can OOM-kill the **serving** pod, not
   just fail itself. Lance's IVF build is designed around a sample-kmeans +
   disk-shuffle, but where that shuffle writes is unspecified here, which risks the
   **tmpfs trap** (a `/tmp` that is RAM-backed turns "spill to disk" into "spill to
   RAM").

The existing scaffolding is worth reusing, not replacing: `src/main.rs` (cold vs
warm, four-phase), `src/bin/first_query_profile.rs` (cold-start cases), and
`src/bin/semantic_cache_profile.rs` already build the in-process stack, drive real
object storage, and emit the `bench/results/` JSON + markdown shape. This RFC adds
**recall** and **constrained build** as peers to those, not a parallel framework.

## Goals

- A reproducible, **in-repo** harness that computes `recall@k` (and `ndcg@k`)
  against **exact-NN ground truth**, runnable from `cargo` against a local /
  MinIO-backed engine. V1 is implemented by `recall.rs` + `recall_sweep.rs`.
- The four requirement axes as four concrete, separable measurements:
  **filtered recall**, **freshness**, **recall-at-latency**, and **build cost
  under a RAM budget**.
- An **index-variant sweep** (`ivf_pq`, `ivf_pq`+`refine_factor`, `ivf_hnsw_sq`,
  `ivf_hnsw_pq`, `ivf_hnsw_flat`) over the search knobs (`nprobes`, `ef`,
  `refine_factor`) — gated on RFC 0009 for the non-PQ knobs, degrading cleanly to
  `ivf_pq` + `nprobes` until then.
- A **constrained-RAM build axis** that records, per (variant × dataset size ×
  RAM budget): peak RSS, OOM point, bytes spilled to NVMe, and build wall-clock —
  so the container size each variant can build under is a measured number.
- Output in the existing `bench/results/` JSON + markdown layout, so results are
  auditable and diffable like the current runs. PR #10's CI wrapper adds the first
  uploaded evidence artifact shape.

## Non-goals

- **The store-vs-store / Turbopuffer harness itself.** That is an edge artifact in
  `../layer` (§ Edge mapping). This RFC builds the engine half and the shared
  loaders/metrics it reuses; it does not add a Turbopuffer client here, and it
  publishes no Turbopuffer numbers (RFC 0086 § Posture, TOS §2.4).
- **Choosing the index.** The harness *informs* the choice (RFC 0009 / the SPFresh
  question); it does not pre-decide it. A variant ships only on evidence, and a
  dataset where it loses is logged, not omitted.
- **Implementing the build-memory knobs.** This RFC *specifies and measures* them
  (NVMe build-scratch, a build-RAM budget, bounded build concurrency); landing them
  is the follow-up the constrained-build numbers justify, and may graduate to its
  own engine RFC.
- **Multivector / late-interaction recall.** The existing BEIR-multivector runs
  stay as-is (RFC 0010); this harness is single-vector ground-truth ANN. Folding
  multivector in is a later extension.
- **A new framework.** Reuse `hevsearch-bench`; do not vendor ann-benchmarks or
  stand up a second harness.

## Design

### Datasets: exact-NN ground truth, not relevance judgments

The harness loads the standard ANN-benchmark datasets, whose ground truth is the
**exact nearest neighbors** of each query (not human relevance qrels):

| Dataset | N × dim | Metric | Role |
|---|---|---|---|
| SIFT1M | 1M × 128 | L2 | entry / fast iteration |
| GIST1M | 1M × 960 | L2 | high-dim stress (PQ width, build RAM) |
| SIFT10M / Deep | 10M+ | L2 | the "larger index" regime; the constrained-build target |

Why ground-truth, not BEIR: BEIR's brute-force `recall@100` caps around **0.83**
on fiqa (`bench/results/.../fiqa_exact_vs_indexed/`) — that ceiling is the
*embedding*, so BEIR conflates index recall with embedding quality. SIFT/GIST
ground truth isolates **pure ANN index recall** (brute force = 1.0 by
construction), which is exactly the index decision this harness exists to make.
Vectors are bring-your-own (the engine's model — `CLAUDE.md`), so no embedding step
is exercised; the harness upserts raw `.fvecs` and scores against the `.ivecs`
ground-truth file.

**Metric note (cross-store):** SIFT/GIST ground truth is **L2**, which matches the
engine's hardcoded single-vector L2. A faithful cross-store run (the edge twin)
needs the namespace's metric to be L2 on every backend — which depends on RFC 0006
(configurable distance metric) for stores that default to cosine. Engine-only runs
are unaffected.

**Loaders:** `.fvecs` / `.bvecs` / `.ivecs` readers (the SIFT/GIST/Deep on-disk
format: little-endian `[dim:i32][dim payload]` records) are implemented in
`recall.rs`; seeded synthetic vectors are implemented for CI-sized runs. A
deterministic **synthetic-attribute generator** that assigns each row categorical
columns (e.g. `category in 0..K`, a date) remains part of the filtered-recall
follow-up so that axis has something to filter on. The generator must be seeded
and reproducible: same dataset, same attributes, same ground truth.

### The four axes = the four requirements

Each requirement is one measurement with an explicit metric, so a result row maps
directly to "does this option meet the bar."

1. **Filtering — filtered recall@k at varying selectivity.** Run each query with a
   predicate of selectivity ≈ {1%, 10%, 50%} (over the synthetic columns), scored
   against **filtered** brute-force ground truth (the exact NN *among rows passing
   the predicate*). This is the test that exposes the prefilter-vs-postfilter cliff:
   a post-filtering index returns fewer than `k` at low selectivity and its
   filtered recall collapses, while the engine's `only_if` **prefilter**
   (`manager.rs:1507`, applied before ranking) should hold recall. Predicates ride
   the existing `filter` field; ground truth is recomputed per predicate.
2. **Updating — the freshness test.** Build the index over a base set, then upsert a
   fresh batch and **query immediately, before any optimize**: measure (a) recall on
   the just-inserted rows (the engine searches the un-indexed tail via a flat scan,
   `manager.rs:1478`, so they should be findable) and (b) the latency cost of that
   flat delta. Then run `optimize` / compact and re-measure: the **incremental-fold
   cost** and recall after. Repeat over several insert→query→optimize cycles to
   catch recall drift and delta-scan latency growth. This is the `shop` / `moment`
   workload (continuous ingest, must serve fresh) made measurable, and it directly
   tests whether `optimize_indices` (`manager.rs:1901`) folds new rows into the
   **vector** index without a retrain, not just the BTree.
3. **Recall — recall@k vs exact ground truth.** The headline: `recall@{1,10,100}`
   and `ndcg@10` against the `.ivecs` ground truth, swept along each variant's
   search knob to produce a **recall-at-latency frontier** (`recall` vs
   `qps` / `p50` / `p95`), with `refine_factor ∈ {none, 2, 5, 10}` where applicable.
   This is where the flat-0.707 PQ ceiling the nprobes sweep documented gets a fair
   re-test on ground-truth data and where `refine_factor`'s lift is quantified.
4. **Operational simplicity — build cost under a RAM budget.** Covered by the
   constrained-build axis below: build wall-clock, **peak RSS**, on-disk index
   size, spill-bytes, and the cold→warm first-query cliff. "Simplicity" is proxied
   by *what it costs to build and operate*, measured rather than asserted.

### Index-variant sweep (gated on RFC 0009)

The sweep matrix is (variant × search-knob × refine), driving the engine REST
directly because **Layer's wire exposes none of these knobs** (RFC 0086 capability
matrix; RFC 0009 § Edge mapping) — the engine harness is the only place they can be
swept:

| Variant | Search knob | Refine | Available |
|---|---|---|---|
| `ivf_pq` (today) | `nprobes` | — | now (`query.rs` `DEFAULT_NPROBES = 20`) |
| `ivf_pq` + refine | `nprobes` | `refine_factor` | RFC 0009 |
| `ivf_hnsw_sq` | `ef` | `refine_factor` | RFC 0009 |
| `ivf_hnsw_pq` | `ef` | `refine_factor` | RFC 0009 |
| `ivf_hnsw_flat` | `ef` | — | RFC 0009 |

Until 0009 lands, the harness runs the first row and records the rest as
"unavailable — gated on RFC 0009," so the gap is visible, never silently dropped.

### Constrained-RAM build axis (the build-memory section)

Index build on container hardware is the constraint we plan for up front. The
architectural premise: **never a global HNSW — the IVF partition is the spill
unit.** IVF_HNSW builds a small graph inside each partition; with
`num_partitions ≈ √N` each partition holds ~√N vectors (≈10 MB at 10M rows), so the
build's working set is a function of in-flight partition concurrency, not dataset
size. Graph edges are cheap (~1.3 GB of layer-0 edges at 10M, M0=32); the memory
cost is random access to **full-precision** vectors during traversal (~30 GB at
10M × 768 × 4) — which partitioning, PQ/SQ codes, and NVMe spill each attack.

**What the axis measures.** For each (variant × dataset size × RAM budget), build
under a **fixed cgroup memory limit** and record:

- **peak RSS** during build (the memory-bound-build signal),
- **OOM point** — the smallest budget at which the build completes vs is killed,
- **spill-bytes to NVMe** (confirms the build actually spills rather than holding
  RAM — and that it is *not* spilling to a tmpfs `/tmp`),
- **build wall-clock**, and the **cold→warm first-query** cliff after build
  (reuse `first_query_profile` machinery).

The budget is enforced the way prod constrains it: run the build bin in a
memory-limited cgroup / container, not via an in-process allocator cap, so the
numbers reflect the real OOM-kill behavior.

**The engine knobs this axis exists to justify and validate** (specified here,
landed as the follow-up the numbers warrant):

| Knob | Why | State |
|---|---|---|
| **NVMe build-scratch dir** | point Lance's partition shuffle / external-merge at the NVMe volume (which today serves only caches), closing the tmpfs trap | gap |
| **Build-RAM budget + bounded partition concurrency** | map the cgroup limit → a target the builder honors by reducing in-flight partitions; without it a build OOM-kills serving | gap |
| **Variant as a memory dial** | `ivf_hnsw_pq` / `_sq` build the graph over PQ/SQ codes (DiskANN-style: compressed vectors drive traversal, full precision stays on NVMe), 4–24× below `ivf_hnsw_flat`, reusing IVF_PQ's codes | free in `lancedb 0.29.0` (RFC 0009) |
| **Build off the serving process** | run the build as an isolated job (a separate bin today; a separate K8s Job on a Karpenter burst node in prod) so a build cannot OOM the serving pod | operational (RFC 0086 deploy shape) |

**Why this is mostly a one-time problem.** Because freshness is incremental
(`optimize_indices` folds new fragments without a full retrain, RFC's § "Updating"),
the full build-memory pressure hits only the **initial bulk load** and forced
rebuilds; steady-state maintenance touches only new fragments and stays bounded. So
the burst-node escape hatch is a once-per-namespace cost, and the algorithmic
levers (NVMe scratch + budget + pq/sq variant) are what let a *constrained*
container do the initial build at all. The harness measures exactly where that
constrained-container ceiling falls per variant.

### Harness architecture & placement

- **Bins in `hevsearch-bench/src/bin/`**, peers to `first_query_profile.rs`:
  `recall_sweep.rs` exists now for axis 3 and the first `ivf_pq` / `nprobes`
  sweep. It reuses the existing in-process `NamespaceService` setup. Follow-ups
  add filtered/freshness modes, HTTP mode where needed, and `build_profile.rs` for
  axis 4, because the constrained-build axis must run the build in its own
  memory-limited process.
- **Ground-truth + recall is shared library code** (`crates/hevsearch-bench/src/recall.rs`),
  so the edge twin in `../layer` can depend on the same scoring and dataset
  loaders rather than reimplementing IR math (which would be a boundary smell —
  `CLAUDE.md` § "don't make Layer reimplement what the engine owns").
- **Datasets** are fetched/cached out of band (a `scripts/` helper), not vendored;
  the harness takes a dataset path. Large datasets (SIFT10M/Deep) are opt-in by
  flag, mirroring the `_100_runs_` / `_aws` skip discipline in CI (`AGENTS.md`).

### Output format

Match the existing `bench/results/` layout. `recall_sweep` currently writes
`bench/results/recall/recall_sweep_<dataset>.json` with
`dataset, rows, dim, num_queries, metric, index, points[]`, per-point `variant`,
`knob`, `qps`, `latency_p50/p95/p99`, and
`scores: { recall@1/10/100, ndcg@10 }`. It also records RFC 0009-gated variants as
unavailable instead of dropping them. PR #10 turns the latest raw JSON into
`bench-evidence.md` and `bench-evidence.json` workflow artifacts with pinned input
metadata.

Build-profile runs will add
`build: { ram_budget_mb, peak_rss_mb, oom: bool, spill_bytes, wall_s, index_bytes }`.
A new `bench/results/recall/` group with a `README.md` mapping files → axes, the
way `beir_multivector_raw/README.md` already documents its runs.

## Edge mapping (how Layer uses this)

The **store-vs-store** comparison — hev search vs Turbopuffer (vs pgvector as the
operational-simplicity yardstick) through the gateway — is the edge twin, and it is
a `../layer` artifact, not built here:

- It drives **Layer's** inbound API, selecting backend per-namespace by
  `VectorStore` CR (`kind: search` vs `kind: turbopuffer`); store-vs-store is the
  same SIFT dataset upserted to two namespaces, queried through one gateway.
- It **reuses this harness's loaders, ground-truth, and recall scoring** (the
  shared `recall` module) so the IR math lives in the engine, once.
- It is **internal / backend-anonymized** — RFC 0086 § Posture and TOS §2.4 forbid
  *published* benchmarks against Turbopuffer. Labels are anonymized from the first
  commit.
- It needs an attribution affordance Layer lacks today: query responses carry no
  backend-identity echo (`x-layer-cache` exists, no `x-vector-store`), so v1
  attributes by construction (one namespace per backend); a routing-echo header is
  a small **Layer** gap to file alongside it.

The index-tuning knobs (`nprobes` / `ef` / `refine_factor` / `kind`) never appear
on Layer's Turbopuffer-shaped wire, which is *why* the variant sweep stays an
engine bench. The edge twin measures "which backend, as the caller sees it"; this
harness measures "which index, and at what build cost."

## Resolved questions

- **Initial recall lane shape:** resolved as source-built, in-process
  `NamespaceService` for v1. Evidence: `recall_sweep.rs` creates
  `NamespaceManager` / `NamespaceService` directly, starts no HTTP server, and PR
  #10 runs that binary.
- **Dataset posture for CI:** resolved as deterministic synthetic defaults for
  scheduled / gate CI, with public `.fvecs` / `.ivecs` inputs available for larger
  local or production-shaped runs. Evidence: `HEVSEARCH_BENCH_ROWS`,
  `HEVSEARCH_BENCH_DIM`, `HEVSEARCH_BENCH_SEED`, and optional
  `HEVSEARCH_BENCH_BASE_FVECS` / `_QUERY_FVECS` / `_GT_IVECS`.
- **RFC 0009 gap handling:** resolved as explicit JSON entries in
  `unavailable_variants` with `"unavailable - gated on RFC 0009"` semantics rather
  than silently omitting those variants.
- **Evidence artifact inputs:** resolved as required immutable `image_tag` and
  `corpus_hash` fields in PR #10, rejecting empty, mutable, `latest`, non-digest,
  and non-mesh-ECR image refs. Evidence: the workflow validation step. The current
  caveat is provenance honesty: the validated image is recorded, not executed.

## Open questions for review

- **Should `bench-evidence` run the pinned image before first publication use, or
  should the provenance field be renamed to source/toolchain identity?** PR #10
  validates and records `image_tag`, but the harness command is
  `cargo run --release -p hevsearch-bench --bin recall_sweep` at the checkout SHA.
  A consumer could otherwise read the artifact as image-executed evidence.
- **The nightly `DEFAULT_IMAGE_TAG` is an all-zero mesh-ECR digest placeholder.**
  Keep nightly inert until docker publishing moves from inherited `ghcr.io` to
  mesh-account ECR, or disable the schedule until a real digest source exists.
- **Does lance 6.0.0's IVF_HNSW build disk-shuffle and stay per-partition-bounded,
  or materialize the whole set?** The load-bearing build-memory unknown; the
  constrained-build axis answers it empirically on the pin.
- **Can Lance's build scratch dir be redirected to the NVMe volume**, and does it
  default to a tmpfs `/tmp`? Confirm before trusting any "spill" number.
- **Ground-truth recompute cost for filtered recall.** Exact filtered NN per
  predicate is O(N) per query; precompute and cache per (dataset × selectivity ×
  seed), or compute a smaller query set. Pick a query count that is stable but cheap.
- **Does `optimize_indices` fold new rows into the *vector* index, or only the
  BTree** (the comment at `manager.rs:1901` is about the scalar index)? The
  freshness axis is partly a test *of* this; if it does not, the freshness story
  needs a delta-tier design (its own question).
- **HTTP-driven vs in-process for future recall axes.** V1 is in-process for speed
  and CI stability. Decide whether filtered/freshness sweeps also stay in-process
  or gain an HTTP mode for deployment fidelity.
- **Where datasets live in CI.** SIFT1M is small enough to fetch; SIFT10M/GIST are
  not. Gate the large ones behind a flag and a cached path.

## Testing

- The harness is itself tested on a **tiny fixture** (a few hundred vectors with a
  known brute-force ground truth computed inline), asserting `recall@k == 1.0`
  against brute force and that a deliberately lossy config scores < 1.0 — so the
  scorer is trusted before any real run.
- Filtered-recall scoring should be unit-tested in the follow-up: filtered brute
  force vs the engine's `only_if` prefilter agree at `recall = 1.0` on the fixture
  for several selectivities.
- Integration runs are `#[ignore]` + MinIO-gated like the rest of the suite
  (`AGENTS.md` § Build & test); the large-dataset runs carry the `_aws` /
  large-data skip markers CI already honors.
- A determinism test exists for synthetic vectors and derived ground truth. The
  filtered-recall follow-up must add the same property for synthetic attributes.

## Dependencies

- **RFC 0009** — the `kind` selector + `ef` / `refine_factor` knobs the variant
  sweep needs; the harness ships `ivf_pq`-only until it lands and is the evidence
  that tells 0009 which variants are worth shipping.
- **RFC 0006** — configurable distance metric, needed only for the cross-store edge
  twin (L2 parity), not for engine-only runs.
- **RFC 0003** — per-row delete, used by the freshness axis to exercise churn
  (insert + delete + optimize), not just append.
- The **build-memory knobs** (NVMe scratch, build-RAM budget, bounded concurrency)
  are surfaced and measured here; landing them is the follow-up these numbers
  justify.

## Alternatives considered

- **Keep recall scoring in the external Python tool.** Rejected — it is
  unversioned, unreproducible from source, and cannot gate a change in CI. The math
  is small; owning it in-repo is the point.
- **Adopt ann-benchmarks wholesale.** Rejected as the core — it assumes a library
  API, not an object-storage REST engine with a foyer cache, and it has no notion of
  filtered recall, the freshness cycle, or a constrained-RAM build. Borrow its
  *datasets and ground-truth format*, not its harness.
- **BEIR-only (extend the multivector runs).** Rejected for the index decision —
  BEIR conflates index recall with embedding quality (the 0.83 brute-force ceiling).
  Keep BEIR for end-to-end quality (RFC 0010); use ground-truth ANN for index
  evaluation.
- **Skip the constrained-build axis; always build on a fat node.** Rejected — the
  user constraint is explicitly "indexing on artificially constrained container
  hardware." The burst-node escape hatch is part of the plan, but the harness must
  still find the constrained-container ceiling, or we discover it in prod.
- **Build the store-vs-store harness here.** Rejected on the engine/edge split —
  the cross-backend comparison drives Layer's wire and is a `../layer` artifact; the
  engine owns the loaders/ground-truth/recall it reuses.

## Fork delta

Pure **additive engine tooling** plus the build-memory knobs the constrained-build
axis justifies — both **local and permanent** on a hard fork, no upstream PR
(`AGENTS.md` § "This is a hard fork"). No subtractive edge removal. The v1 harness
rides the existing `hevsearch-bench` crate and the `bench/results/` layout; the new
dependency surface is engine-local dataset/scoring code and, in a follow-up, a
cgroup-limited build runner.

## References

- `crates/hevsearch-bench/src/main.rs` — the latency/cache harness (synthetic
  `make_vector`, in-process `NamespaceService`, IVF params `√rows` / `dim/16`);
  `src/bin/first_query_profile.rs`, `src/bin/semantic_cache_profile.rs` — the
  scaffolding the new bins mirror.
- `crates/hevsearch-bench/src/recall.rs` — implemented RFC 0011 primitives:
  `.fvecs` / `.bvecs` / `.ivecs` readers, exact L2 ground truth,
  `recall@k` / `ndcg@k`, deterministic synthetic vectors, and scorer tests.
- `crates/hevsearch-bench/src/bin/recall_sweep.rs` — implemented v1 recall lane:
  synthetic or `.fvecs` inputs, linear-scan self-check, `ivf_pq` `nprobes` sweep,
  JSON output under `bench/results/recall/`, and RFC 0009-gated variant markers.
- PR #10 (`feat/bench-evidence-ci`) — the workflow wrapper that validates
  `image_tag` / `corpus_hash`, runs the v1 harness against MinIO, and uploads
  bench evidence artifacts; it records, but does not execute, the pinned image.
- `bench/results/beir_multivector_raw/README.md` + `quality_0.9.2/` — the current
  (externally-scored) quality runs and the JSON shape to match;
  `fiqa_exact_vs_indexed/` — the 0.83 brute-force ceiling that motivates
  ground-truth datasets; `nprobes_sweep_fiqa/` — the flat-0.707 PQ ceiling the
  recall axis re-tests.
- `crates/hevsearch-core/src/manager.rs:1789-1801` — `create_index` (default
  `IvfPqIndexBuilder`, no memory/scratch options); `:1478` (flat scan over the
  un-indexed tail — the freshness path); `:1507` (`only_if` prefilter — the
  filtered-recall path); `:1901` (`optimize_indices` absorbs new rows without
  retrain); `:1974` (`compact` → `OptimizeAction::default()`).
- `crates/hevsearch-api/src/handlers.rs:413` — index build is an in-process
  `tokio::spawn` on the serving service (the OOM-the-serving-pod risk); `:390` — the
  `kind != "ivf_pq"` rejection RFC 0009 lifts.
- `crates/hevsearch-core/src/cache/layer.rs`, `object_cache.rs` — NVMe wired for
  caches only, not build scratch.
- `Cargo.toml:24-25` — `lancedb = "=0.29.0"` / `lance = "=6.0.0"` (the HNSW
  builders and `ef` / `refine_factor` the sweep needs; no bump).
- `docs/rfcs/0009-pluggable-vector-index-type.md` — the variant selector + query
  knobs this harness sweeps and validates; `0006` — distance metric (cross-store
  parity); `0003` — per-row delete (freshness churn); `0010` — multivector quality
  (kept separate).
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the edge twin, the
  per-namespace backend selection, the §2.4 / Posture constraint on Turbopuffer
  benchmarks, and the Karpenter/KEDA deploy shape the burst build node rides.
- `CLAUDE.md` § "search owns the engine"; `AGENTS.md` § "Where findings go" /
  "Build & test".
