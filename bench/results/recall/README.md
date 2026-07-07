# Recall harness results (RFC 0011)

Output of `recall_sweep` (`crates/hevsearch-bench/src/bin/recall_sweep.rs`) —
`recall@{1,10,100}` / `ndcg@10` against **exact-NN ground truth**, per index
variant and search-knob point, with qps and latency percentiles.

- `recall_sweep_<dataset>.json` — one file per dataset run. `points` carries the
  swept rows (the `linear_scan` row is the exact baseline and must score
  recall ≈ 1.0 — it is the scorer's self-check); `unavailable_variants` lists
  the RFC 0009-gated variants (`ivf_pq`+refine, `ivf_hnsw_*`) not yet runnable,
  so the gap is visible rather than silently dropped.

Datasets: fetch with `scripts/fetch_ann_datasets sift1m` (SIFT/GIST ground
truth is L2, matching the engine's single-vector default). With no dataset
env vars set the harness generates a seeded synthetic dataset and computes
brute-force ground truth in-process — reproducible (same seed → same ground
truth) but not comparable across seeds/sizes.

Axes still to land from RFC 0011: filtered recall at varying selectivity,
freshness (insert → query-before-optimize → fold), and the constrained-RAM
build profile (`build_profile` bin).
