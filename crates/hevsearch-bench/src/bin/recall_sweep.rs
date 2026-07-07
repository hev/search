//! Recall sweep harness (RFC 0011, axis 3: recall-at-latency).
//!
//! Loads a ground-truth ANN dataset — SIFT/GIST-style `.fvecs` +
//! `.ivecs`, or a seeded synthetic dataset with brute-force ground
//! truth computed in-process — upserts it through the real
//! `NamespaceService`, and measures `recall@{1,10,100}` / `ndcg@10`
//! against the exact nearest neighbours:
//!
//! 1. **Exact baseline** — a linear-scan (unindexed) namespace, which
//!    must score recall ≈ 1.0. This self-calibrates the scorer before
//!    any indexed number is trusted.
//! 2. **`ivf_pq` sweep** — build the index, then sweep `nprobes`,
//!    recording recall, ndcg, qps, and latency percentiles per point.
//!
//! The RFC 0009 variants (`ivf_pq`+refine, `ivf_hnsw_sq`, `ivf_hnsw_pq`,
//! `ivf_hnsw_flat`) are recorded in the output as
//! `"unavailable — gated on RFC 0009"` rather than silently dropped.
//!
//! Run (synthetic, MinIO):
//!
//! ```text
//! docker compose up -d minio minio-init
//! HEVSEARCH_STORAGE_URI=s3://hevsearch-test \
//! HEVSEARCH_S3_ENDPOINT=http://127.0.0.1:9000 \
//! HEVSEARCH_S3_ACCESS_KEY=minioadmin \
//! HEVSEARCH_S3_SECRET_KEY=minioadmin \
//! HEVSEARCH_BENCH_ROWS=100000 HEVSEARCH_BENCH_DIM=128 \
//!   ./scripts/cargo run --release -p hevsearch-bench --bin recall_sweep
//! ```
//!
//! Run (SIFT1M — fetch with `scripts/fetch_ann_datasets`):
//!
//! ```text
//! HEVSEARCH_BENCH_BASE_FVECS=datasets/sift/sift_base.fvecs \
//! HEVSEARCH_BENCH_QUERY_FVECS=datasets/sift/sift_query.fvecs \
//! HEVSEARCH_BENCH_GT_IVECS=datasets/sift/sift_groundtruth.ivecs \
//!   ... recall_sweep
//! ```
//!
//! | var | default |
//! | --- | ------- |
//! | `HEVSEARCH_STORAGE_URI` / `HEVSEARCH_S3_BUCKET` | *(required)* |
//! | `HEVSEARCH_BENCH_BASE_FVECS` / `_QUERY_FVECS` / `_GT_IVECS` | *(unset → synthetic)* |
//! | `HEVSEARCH_BENCH_ROWS` | `10000` (synthetic) |
//! | `HEVSEARCH_BENCH_DIM` | `128` (synthetic) |
//! | `HEVSEARCH_BENCH_QUERIES` | `100` |
//! | `HEVSEARCH_BENCH_SEED` | `42` (synthetic) |
//! | `HEVSEARCH_BENCH_NPROBES_SWEEP` | `1,8,20,50,100` |
//! | `HEVSEARCH_BENCH_PARTITIONS` | `sqrt(rows)` |
//! | `HEVSEARCH_BENCH_SUB_VECTORS` | `dim/16` |
//! | `HEVSEARCH_BENCH_NUM_BITS` | (LanceDB default) |
//! | `HEVSEARCH_BENCH_SKIP_EXACT` | unset (set to skip the linear baseline) |
//! | `HEVSEARCH_BENCH_OUT_DIR` | `bench/results/recall` |

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use hevsearch_bench::recall::{
    brute_force_knn, mean, ndcg_at_k, read_fvecs, read_ivecs, recall_at_k, synthetic_vectors,
};
use hevsearch_core::cache::NamespaceCache;
use hevsearch_core::{
    CoreMetrics, NamespaceId, NamespaceManager, NamespaceService, QueryRequest, RowId, StorageRoot,
    UpsertRow,
};

const RECALL_KS: [usize; 3] = [1, 10, 100];
const NDCG_K: usize = 10;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

struct Dataset {
    label: String,
    base: Vec<Vec<f32>>,
    queries: Vec<Vec<f32>>,
    /// Exact NN ids per query, nearest first, ≥ 100 deep.
    ground_truth: Vec<Vec<u32>>,
}

fn load_dataset() -> anyhow::Result<Dataset> {
    let num_queries: usize = env_or("HEVSEARCH_BENCH_QUERIES", "100")
        .parse()
        .context("HEVSEARCH_BENCH_QUERIES")?;
    let gt_depth = *RECALL_KS.iter().max().unwrap();

    if let Ok(base_path) = std::env::var("HEVSEARCH_BENCH_BASE_FVECS") {
        let query_path = std::env::var("HEVSEARCH_BENCH_QUERY_FVECS")
            .context("HEVSEARCH_BENCH_QUERY_FVECS must be set with _BASE_FVECS")?;
        let gt_path = std::env::var("HEVSEARCH_BENCH_GT_IVECS")
            .context("HEVSEARCH_BENCH_GT_IVECS must be set with _BASE_FVECS")?;
        let base = read_fvecs(PathBuf::from(&base_path).as_path(), None)?;
        let queries = read_fvecs(PathBuf::from(&query_path).as_path(), Some(num_queries))?;
        let ground_truth = read_ivecs(PathBuf::from(&gt_path).as_path(), Some(num_queries))?;
        anyhow::ensure!(
            ground_truth.iter().all(|g| g.len() >= gt_depth),
            "ground truth shallower than {gt_depth}"
        );
        let label = PathBuf::from(&base_path)
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "dataset".into());
        Ok(Dataset {
            label,
            base,
            queries,
            ground_truth,
        })
    } else {
        let rows: usize = env_or("HEVSEARCH_BENCH_ROWS", "10000")
            .parse()
            .context("HEVSEARCH_BENCH_ROWS")?;
        let dim: usize = env_or("HEVSEARCH_BENCH_DIM", "128")
            .parse()
            .context("HEVSEARCH_BENCH_DIM")?;
        let seed: u64 = env_or("HEVSEARCH_BENCH_SEED", "42")
            .parse()
            .context("HEVSEARCH_BENCH_SEED")?;
        anyhow::ensure!(rows > gt_depth, "need more than {gt_depth} rows");
        let base = synthetic_vectors(seed, rows, dim);
        let queries = synthetic_vectors(seed.wrapping_add(1), num_queries, dim);
        eprintln!(
            "computing brute-force ground truth for {num_queries} queries over {rows} rows..."
        );
        let ground_truth = queries
            .iter()
            .map(|q| brute_force_knn(&base, q, gt_depth))
            .collect();
        Ok(Dataset {
            label: format!("synthetic-seed{seed}-{rows}x{dim}"),
            base,
            queries,
            ground_truth,
        })
    }
}

struct SweepPoint {
    variant: &'static str,
    knob: String,
    recall: [f64; 3],
    ndcg10: f64,
    qps: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

async fn run_point(
    service: &NamespaceService,
    ns: &NamespaceId,
    ds: &Dataset,
    nprobes: Option<usize>,
    variant: &'static str,
    knob: String,
) -> anyhow::Result<SweepPoint> {
    let k = *RECALL_KS.iter().max().unwrap();
    let mut latencies = Vec::with_capacity(ds.queries.len());
    let mut recalls: [Vec<f64>; 3] = Default::default();
    let mut ndcgs = Vec::with_capacity(ds.queries.len());
    let started = Instant::now();
    for (qi, q) in ds.queries.iter().enumerate() {
        let req = QueryRequest {
            vector: q.clone(),
            vectors: None,
            k,
            nprobes,
            text: None,
            fuzzy: None,
            filter: None,
            include_vector: false,
            semantic_cache: None,
        };
        let t = Instant::now();
        let res = service.query(ns, &req).await?;
        latencies.push(t.elapsed());
        let ids: Vec<u64> = res
            .results
            .iter()
            .filter_map(|r| match &r.id {
                RowId::U64(id) => Some(*id),
                RowId::String(_) => None,
            })
            .collect();
        let gt = &ds.ground_truth[qi];
        for (slot, &rk) in RECALL_KS.iter().enumerate() {
            recalls[slot].push(recall_at_k(gt, &ids, rk));
        }
        ndcgs.push(ndcg_at_k(gt, &ids, NDCG_K));
    }
    let wall = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let n = latencies.len();
    Ok(SweepPoint {
        variant,
        knob,
        recall: [mean(&recalls[0]), mean(&recalls[1]), mean(&recalls[2])],
        ndcg10: mean(&ndcgs),
        qps: n as f64 / wall,
        p50: latencies[n / 2],
        p95: latencies[(n * 95) / 100],
        p99: latencies[(n * 99) / 100],
    })
}

async fn upsert_base(
    service: &NamespaceService,
    ns: &NamespaceId,
    base: &[Vec<f32>],
) -> anyhow::Result<Duration> {
    let start = Instant::now();
    for chunk_start in (0..base.len()).step_by(10_000) {
        let end = (chunk_start + 10_000).min(base.len());
        let rows: Vec<UpsertRow> = (chunk_start..end)
            .map(|i| (i as u64, base[i].clone()).into())
            .collect();
        service.upsert(ns, rows).await?;
    }
    Ok(start.elapsed())
}

fn storage_from_env() -> anyhow::Result<(StorageRoot, HashMap<String, String>)> {
    let uri = std::env::var("HEVSEARCH_STORAGE_URI")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|s| !s.is_empty());
    let bucket = std::env::var("HEVSEARCH_S3_BUCKET")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|s| !s.is_empty());
    let root = match (uri, bucket) {
        (Some(uri), _) => StorageRoot::parse(&uri)
            .map_err(|e| anyhow::anyhow!("HEVSEARCH_STORAGE_URI ({uri:?}): {e}"))?,
        (None, Some(bucket)) => StorageRoot::s3_bucket(&bucket)
            .map_err(|e| anyhow::anyhow!("HEVSEARCH_S3_BUCKET ({bucket:?}): {e}"))?,
        (None, None) => anyhow::bail!(
            "set HEVSEARCH_STORAGE_URI=s3://bucket or the legacy HEVSEARCH_S3_BUCKET=bucket"
        ),
    };
    let mut opts = HashMap::new();
    if let Ok(v) = std::env::var("HEVSEARCH_S3_ENDPOINT") {
        opts.insert("aws_endpoint".into(), v);
        opts.insert("allow_http".into(), "true".into());
        opts.insert("aws_virtual_hosted_style_request".into(), "false".into());
    }
    if let Ok(v) = std::env::var("HEVSEARCH_S3_ACCESS_KEY") {
        opts.insert("aws_access_key_id".into(), v);
    }
    if let Ok(v) = std::env::var("HEVSEARCH_S3_SECRET_KEY") {
        opts.insert("aws_secret_access_key".into(), v);
    }
    opts.insert(
        "aws_region".into(),
        hevsearch_core::resolve_s3_region(|k| std::env::var(k).ok()),
    );
    Ok((root, opts))
}

fn ts_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (storage_root, storage_options) = storage_from_env()?;
    let ds = load_dataset()?;
    let dim = ds.base[0].len();
    let rows = ds.base.len();
    let nprobes_sweep: Vec<usize> = env_or("HEVSEARCH_BENCH_NPROBES_SWEEP", "1,8,20,50,100")
        .split(',')
        .map(|s| s.trim().parse().context("HEVSEARCH_BENCH_NPROBES_SWEEP"))
        .collect::<anyhow::Result<_>>()?;
    let num_partitions: u32 = match std::env::var("HEVSEARCH_BENCH_PARTITIONS") {
        Ok(v) => v.parse().context("HEVSEARCH_BENCH_PARTITIONS")?,
        Err(_) => (rows as f64).sqrt() as u32,
    };
    let num_sub_vectors: u32 = match std::env::var("HEVSEARCH_BENCH_SUB_VECTORS") {
        Ok(v) => v.parse().context("HEVSEARCH_BENCH_SUB_VECTORS")?,
        Err(_) => (dim / 16).max(1) as u32,
    };
    let num_bits: Option<u32> = std::env::var("HEVSEARCH_BENCH_NUM_BITS")
        .ok()
        .map(|v| v.parse().context("HEVSEARCH_BENCH_NUM_BITS"))
        .transpose()?;
    let skip_exact = std::env::var("HEVSEARCH_BENCH_SKIP_EXACT").is_ok();
    let out_dir = PathBuf::from(env_or("HEVSEARCH_BENCH_OUT_DIR", "bench/results/recall"));

    println!(
        "recall sweep: dataset={} rows={rows} dim={dim} queries={} \
         partitions={num_partitions} sub_vectors={num_sub_vectors} nprobes={nprobes_sweep:?}",
        ds.label,
        ds.queries.len(),
    );

    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CoreMetrics::new()?);
    let manager = Arc::new(NamespaceManager::new(
        storage_root,
        storage_options,
        Arc::clone(&metrics),
    ));
    let cache = Arc::new(
        NamespaceCache::new(64 * 1024 * 1024, tmp.path(), 512 * 1024 * 1024, Arc::clone(&metrics))
            .await
            .map_err(|e| anyhow::anyhow!("build cache: {e}"))?,
    );
    let service = NamespaceService::new(Arc::clone(&manager), cache, Arc::clone(&metrics));

    let mut points: Vec<SweepPoint> = Vec::new();

    // ---- exact baseline: linear scan must score recall ≈ 1.0 ----
    if !skip_exact {
        let ns = NamespaceId::new(format!("recall-exact-{}", ts_nanos()))?;
        println!("\n--- exact baseline (linear scan): {ns} ---");
        let up = upsert_base(&service, &ns, &ds.base).await?;
        println!("  upsert {:.1}s", up.as_secs_f64());
        let point = run_point(&service, &ns, &ds, None, "linear_scan", "-".into()).await?;
        println!(
            "  recall@100={:.4} (must be ≈ 1.0 — the scorer self-check)",
            point.recall[2]
        );
        if point.recall[2] < 0.999 {
            eprintln!(
                "WARNING: exact baseline recall@100 = {:.4} — scorer or \
                 flat-scan path is suspect; indexed numbers below are untrusted",
                point.recall[2]
            );
        }
        points.push(point);
    }

    // ---- ivf_pq nprobes sweep ----
    let ns = NamespaceId::new(format!("recall-ivfpq-{}", ts_nanos()))?;
    println!("\n--- ivf_pq sweep: {ns} ---");
    let up = upsert_base(&service, &ns, &ds.base).await?;
    println!("  upsert {:.1}s", up.as_secs_f64());
    let build_start = Instant::now();
    service
        .create_index(&ns, Some(num_partitions), Some(num_sub_vectors), num_bits)
        .await?;
    let build = build_start.elapsed();
    println!("  index build {:.1}s", build.as_secs_f64());
    for &np in &nprobes_sweep {
        let point = run_point(&service, &ns, &ds, Some(np), "ivf_pq", format!("nprobes={np}"))
            .await?;
        println!(
            "  nprobes={np:>4}  recall@1={:.4} @10={:.4} @100={:.4}  ndcg@10={:.4}  \
             qps={:.1}  p50={:.1}ms p95={:.1}ms",
            point.recall[0],
            point.recall[1],
            point.recall[2],
            point.ndcg10,
            point.qps,
            point.p50.as_secs_f64() * 1e3,
            point.p95.as_secs_f64() * 1e3,
        );
        points.push(point);
    }

    // ---- write results ----
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("creating {}", out_dir.display()))?;
    // Hand-rolled JSON: the structure is small and fixed, and the
    // bench crate deliberately carries no serde_json dependency.
    // Every string interpolated below is either a knob literal or a
    // dataset label derived from a file stem / numeric config — no
    // arbitrary user text, so no escaping is required.
    let points_json: Vec<String> = points
        .iter()
        .map(|p| {
            format!(
                "    {{\n      \"variant\": \"{}\",\n      \"knob\": \"{}\",\n      \
                 \"scores\": {{ \"recall@1\": {:.6}, \"recall@10\": {:.6}, \
                 \"recall@100\": {:.6}, \"ndcg@10\": {:.6} }},\n      \
                 \"qps\": {:.2},\n      \"latency_p50_ms\": {:.3}, \
                 \"latency_p95_ms\": {:.3}, \"latency_p99_ms\": {:.3}\n    }}",
                p.variant,
                p.knob,
                p.recall[0],
                p.recall[1],
                p.recall[2],
                p.ndcg10,
                p.qps,
                p.p50.as_secs_f64() * 1e3,
                p.p95.as_secs_f64() * 1e3,
                p.p99.as_secs_f64() * 1e3,
            )
        })
        .collect();
    let gated_json: Vec<String> = ["ivf_pq+refine", "ivf_hnsw_sq", "ivf_hnsw_pq", "ivf_hnsw_flat"]
        .iter()
        .map(|v| {
            format!(
                "    {{ \"variant\": \"{v}\", \"status\": \"unavailable — gated on RFC 0009\" }}"
            )
        })
        .collect();
    let json = format!(
        "{{\n  \"dataset\": \"{}\",\n  \"rows\": {rows},\n  \"dim\": {dim},\n  \
         \"num_queries\": {},\n  \"metric\": \"l2\",\n  \"index\": {{ \"kind\": \"ivf_pq\", \
         \"num_partitions\": {num_partitions}, \"num_sub_vectors\": {num_sub_vectors}, \
         \"num_bits\": {}, \"build_wall_s\": {:.2} }},\n  \"points\": [\n{}\n  ],\n  \
         \"unavailable_variants\": [\n{}\n  ]\n}}\n",
        ds.label,
        ds.queries.len(),
        num_bits.map_or("null".into(), |b| b.to_string()),
        build.as_secs_f64(),
        points_json.join(",\n"),
        gated_json.join(",\n"),
    );
    let out_path = out_dir.join(format!("recall_sweep_{}.json", ds.label));
    std::fs::write(&out_path, json)
        .with_context(|| format!("writing {}", out_path.display()))?;
    println!("\nwrote {}", out_path.display());
    Ok(())
}
