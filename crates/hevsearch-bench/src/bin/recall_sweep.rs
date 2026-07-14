//! Recall sweep harness (RFC 0011, axis 3: recall-at-latency).
//!
//! Loads an ANN query set — SIFT/GIST-style `.fvecs`, or a seeded
//! synthetic dataset — upserts it through the real `NamespaceService`,
//! runs `exact: true` queries through the same engine path to compute
//! reference answers, and measures indexed `recall@{1,10,100}` /
//! `ndcg@10` against that in-engine exact reference:
//!
//! 1. **Exact reference** — `exact: true` queries on the same indexed
//!    namespace, bypassing the vector index while preserving metric,
//!    filter, projection, and engine row-id behavior.
//! 2. **`ivf_pq` sweep** — sweep indexed `nprobes`,
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
//!   ... recall_sweep
//! ```
//!
//! | var | default |
//! | --- | ------- |
//! | `HEVSEARCH_STORAGE_URI` / `HEVSEARCH_S3_BUCKET` | *(required)* |
//! | `HEVSEARCH_BENCH_BASE_FVECS` / `_QUERY_FVECS` | *(unset → synthetic)* |
//! | `HEVSEARCH_BENCH_ROWS` | `10000` (synthetic) |
//! | `HEVSEARCH_BENCH_DIM` | `128` (synthetic) |
//! | `HEVSEARCH_BENCH_QUERIES` | `100` |
//! | `HEVSEARCH_BENCH_SEED` | `42` (synthetic) |
//! | `HEVSEARCH_BENCH_NPROBES_SWEEP` | `1,8,20,50,100` |
//! | `HEVSEARCH_BENCH_PARTITIONS` | `sqrt(rows)` |
//! | `HEVSEARCH_BENCH_SUB_VECTORS` | `dim/16` |
//! | `HEVSEARCH_BENCH_NUM_BITS` | (LanceDB default) |
//! | `HEVSEARCH_BENCH_OUT_DIR` | `bench/results/recall` |

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use hevsearch_bench::recall::{
    mean, ndcg_at_k_ids, read_fvecs, recall_at_k_ids, synthetic_vectors,
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
}

fn load_dataset() -> anyhow::Result<Dataset> {
    let num_queries: usize = env_or("HEVSEARCH_BENCH_QUERIES", "100")
        .parse()
        .context("HEVSEARCH_BENCH_QUERIES")?;

    if let Ok(base_path) = std::env::var("HEVSEARCH_BENCH_BASE_FVECS") {
        let query_path = std::env::var("HEVSEARCH_BENCH_QUERY_FVECS")
            .context("HEVSEARCH_BENCH_QUERY_FVECS must be set with _BASE_FVECS")?;
        let base = read_fvecs(PathBuf::from(&base_path).as_path(), None)?;
        let queries = read_fvecs(PathBuf::from(&query_path).as_path(), Some(num_queries))?;
        let label = PathBuf::from(&base_path)
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "dataset".into());
        Ok(Dataset {
            label,
            base,
            queries,
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
        anyhow::ensure!(
            rows > *RECALL_KS.iter().max().unwrap(),
            "need more rows than max recall k"
        );
        let base = synthetic_vectors(seed, rows, dim);
        let queries = synthetic_vectors(seed.wrapping_add(1), num_queries, dim);
        Ok(Dataset {
            label: format!("synthetic-seed{seed}-{rows}x{dim}"),
            base,
            queries,
        })
    }
}

struct QuerySet {
    label: &'static str,
    filter: Option<String>,
}

struct ExactReference {
    query_set: &'static str,
    ids: Vec<Vec<u64>>,
    qps: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

struct SweepPoint {
    variant: &'static str,
    query_set: &'static str,
    knob: String,
    recall: [f64; 3],
    ndcg10: f64,
    qps: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

struct PointContext<'a> {
    reference: &'a ExactReference,
    query_set: &'a QuerySet,
    variant: &'static str,
    knob: String,
}

async fn run_queries(
    service: &NamespaceService,
    ns: &NamespaceId,
    queries: &[Vec<f32>],
    k: usize,
    nprobes: Option<usize>,
    exact: bool,
    filter: Option<&str>,
) -> anyhow::Result<(Vec<Vec<u64>>, f64, Duration, Duration, Duration)> {
    let mut latencies = Vec::with_capacity(queries.len());
    let mut all_ids = Vec::with_capacity(queries.len());
    let started = Instant::now();
    for q in queries {
        let req = QueryRequest {
            vector: q.clone(),
            vectors: None,
            k,
            nprobes,
            exact,
            text: None,
            fuzzy: None,
            filter: filter.map(str::to_string),
            include_vector: false,
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
        all_ids.push(ids);
    }
    let wall = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let n = latencies.len();
    Ok((
        all_ids,
        n as f64 / wall,
        latencies[n / 2],
        latencies[(n * 95) / 100],
        latencies[(n * 99) / 100],
    ))
}

async fn collect_exact_reference(
    service: &NamespaceService,
    ns: &NamespaceId,
    ds: &Dataset,
    query_set: &QuerySet,
) -> anyhow::Result<ExactReference> {
    let k = *RECALL_KS.iter().max().unwrap();
    let (ids, qps, p50, p95, p99) = run_queries(
        service,
        ns,
        &ds.queries,
        k,
        None,
        true,
        query_set.filter.as_deref(),
    )
    .await?;
    anyhow::ensure!(
        ids.iter().all(|hits| hits.len() >= k),
        "exact reference returned fewer than {k} hits; lower k or use a larger filtered query set"
    );
    Ok(ExactReference {
        query_set: query_set.label,
        ids,
        qps,
        p50,
        p95,
        p99,
    })
}

async fn run_point(
    service: &NamespaceService,
    ns: &NamespaceId,
    ds: &Dataset,
    nprobes: Option<usize>,
    ctx: PointContext<'_>,
) -> anyhow::Result<SweepPoint> {
    let k = *RECALL_KS.iter().max().unwrap();
    let mut recalls: [Vec<f64>; 3] = Default::default();
    let mut ndcgs = Vec::with_capacity(ds.queries.len());
    let (retrieved, qps, p50, p95, p99) = run_queries(
        service,
        ns,
        &ds.queries,
        k,
        nprobes,
        false,
        ctx.query_set.filter.as_deref(),
    )
    .await?;
    for (qi, ids) in retrieved.iter().enumerate() {
        let gt = &ctx.reference.ids[qi];
        for (slot, &rk) in RECALL_KS.iter().enumerate() {
            recalls[slot].push(recall_at_k_ids(gt, ids, rk));
        }
        ndcgs.push(ndcg_at_k_ids(gt, ids, NDCG_K));
    }
    Ok(SweepPoint {
        variant: ctx.variant,
        query_set: ctx.query_set.label,
        knob: ctx.knob,
        recall: [mean(&recalls[0]), mean(&recalls[1]), mean(&recalls[2])],
        ndcg10: mean(&ndcgs),
        qps,
        p50,
        p95,
        p99,
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
        NamespaceCache::new(
            64 * 1024 * 1024,
            tmp.path(),
            512 * 1024 * 1024,
            Arc::clone(&metrics),
        )
        .await
        .map_err(|e| anyhow::anyhow!("build cache: {e}"))?,
    );
    let service = NamespaceService::new(Arc::clone(&manager), cache, Arc::clone(&metrics));

    let mut points: Vec<SweepPoint> = Vec::new();
    let query_sets = vec![QuerySet {
        label: "unfiltered",
        filter: None,
    }];

    // ---- ivf_pq nprobes sweep, scored against exact mode on the same namespace ----
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
    let mut references = Vec::new();
    for query_set in &query_sets {
        let reference = collect_exact_reference(&service, &ns, &ds, query_set).await?;
        println!(
            "  exact reference ({}) qps={:.1} p50={:.1}ms p95={:.1}ms",
            reference.query_set,
            reference.qps,
            reference.p50.as_secs_f64() * 1e3,
            reference.p95.as_secs_f64() * 1e3,
        );
        references.push(reference);
    }
    for &np in &nprobes_sweep {
        for (query_set, reference) in query_sets.iter().zip(&references) {
            let point = run_point(
                &service,
                &ns,
                &ds,
                Some(np),
                PointContext {
                    reference,
                    query_set,
                    variant: "ivf_pq",
                    knob: format!("nprobes={np}"),
                },
            )
            .await?;
            println!(
                "  {} nprobes={np:>4}  recall@1={:.4} @10={:.4} @100={:.4}  ndcg@10={:.4}  \
                 qps={:.1}  p50={:.1}ms p95={:.1}ms",
                point.query_set,
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
    }

    // ---- write results ----
    std::fs::create_dir_all(&out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
    let json = serde_json::json!({
        "dataset": ds.label,
        "rows": rows,
        "dim": dim,
        "num_queries": ds.queries.len(),
        "metric": "l2",
        "ground_truth_source": "in_engine_exact",
        "ground_truth_mode": {
            "exact": true,
            "same_namespace": ns.as_str(),
            "filtered_strategy": "pending hev/search#25; this run uses only the unfiltered query set"
        },
        "index": {
            "kind": "ivf_pq",
            "num_partitions": num_partitions,
            "num_sub_vectors": num_sub_vectors,
            "num_bits": num_bits,
            "build_wall_s": build.as_secs_f64(),
        },
        "exact_reference": references.iter().map(|r| serde_json::json!({
            "query_set": r.query_set,
            "qps": r.qps,
            "latency_p50_ms": r.p50.as_secs_f64() * 1e3,
            "latency_p95_ms": r.p95.as_secs_f64() * 1e3,
            "latency_p99_ms": r.p99.as_secs_f64() * 1e3,
        })).collect::<Vec<_>>(),
        "points": points.iter().map(|p| serde_json::json!({
            "variant": p.variant,
            "query_set": p.query_set,
            "knob": p.knob,
            "scored_against": "in_engine_exact",
            "scores": {
                "recall@1": p.recall[0],
                "recall@10": p.recall[1],
                "recall@100": p.recall[2],
                "ndcg@10": p.ndcg10,
            },
            "qps": p.qps,
            "latency_p50_ms": p.p50.as_secs_f64() * 1e3,
            "latency_p95_ms": p.p95.as_secs_f64() * 1e3,
            "latency_p99_ms": p.p99.as_secs_f64() * 1e3,
        })).collect::<Vec<_>>(),
        "unavailable_variants": [
            { "variant": "ivf_pq+refine", "status": "unavailable - gated on RFC 0009" },
            { "variant": "ivf_hnsw_sq", "status": "unavailable - gated on RFC 0009" },
            { "variant": "ivf_hnsw_pq", "status": "unavailable - gated on RFC 0009" },
            { "variant": "ivf_hnsw_flat", "status": "unavailable - gated on RFC 0009" },
        ],
    });
    let out_path = out_dir.join(format!("recall_sweep_{}.json", ds.label));
    std::fs::write(&out_path, serde_json::to_string_pretty(&json)? + "\n")
        .with_context(|| format!("writing {}", out_path.display()))?;
    println!("\nwrote {}", out_path.display());
    Ok(())
}
