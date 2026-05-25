//! Index-cache benchmark harness.
//!
//! Runs 1000 novel-vector queries against a single seeded
//! namespace and reports query latency, cache-backend traffic,
//! and Firn-level S3 request deltas for one of three modes:
//!
//! - `current-firn`: production result cache, no Lance index
//!   cache. The behaviour Firn ships today.
//! - `warm-index-cache`: production result cache plus a
//!   `FoyerCacheBackend` installed on the `lancedb::Session`. A
//!   100-query warmup pass (this mode only) populates the index
//!   cache before the 1000-query measurement window. Other modes
//!   skip the warmup so the baseline does not warm Lance's
//!   default in-memory session cache.
//! - `cache-disabled-cold`: bypasses the service-level result
//!   cache. Calls `NamespaceManager::query` directly. Useful as
//!   a sanity check that the result cache is not silently
//!   carrying weight on a "novel" workload (it should not, because
//!   every query is fresh, but better to confirm than assume).
//!
//! Every query is a freshly seeded random vector, so the
//! service-level result cache never hits. The variable being
//! measured is whether Lance's index opens and metadata reads
//! are served from foyer or from S3.
//!
//! Defaults match the prototype plan: 1M rows, 1536-dim vectors,
//! 1000 measured queries (plus a 100-query warmup on the warm
//! mode), 1 GiB RAM and 10 GiB NVMe for the foyer index cache.
//! Override via the env vars in the table below.
//!
//! Run with:
//!
//! ```text
//! docker compose up -d minio minio-init
//! FIRNFLOW_STORAGE_URI=s3://firnflow-test \
//! FIRNFLOW_S3_ENDPOINT=http://127.0.0.1:9000 \
//! FIRNFLOW_S3_ACCESS_KEY=minioadmin \
//! FIRNFLOW_S3_SECRET_KEY=minioadmin \
//! FIRNFLOW_BENCH_INDEX_CACHE_MODE=warm-index-cache \
//!   ./scripts/cargo run --release -p firnflow-bench --bin index_cache_bench
//! ```
//!
//! | var                                  | default                                        |
//! | ------------------------------------ | ---------------------------------------------- |
//! | `FIRNFLOW_BENCH_INDEX_CACHE_MODE`    | `current-firn`                                 |
//! | `FIRNFLOW_BENCH_INDEX_CACHE_NS`      | `index-cache-bench` (stable across runs)       |
//! | `FIRNFLOW_BENCH_DIM`                 | `1536`                                         |
//! | `FIRNFLOW_BENCH_ROWS`                | `1000000`                                      |
//! | `FIRNFLOW_BENCH_QUERIES`             | `1000`                                         |
//! | `FIRNFLOW_BENCH_WARMUP`              | `100`                                          |
//! | `FIRNFLOW_BENCH_K`                   | `10`                                           |
//! | `FIRNFLOW_BENCH_INDEX_NPROBES`       | `20`                                           |
//! | `FIRNFLOW_BENCH_INDEX_CACHE_RAM_MB`  | `1024`                                         |
//! | `FIRNFLOW_BENCH_INDEX_CACHE_NVME_MB` | `10240`                                        |
//! | `FIRNFLOW_BENCH_RESULT_CACHE_RAM_MB` | `16`                                           |
//! | `FIRNFLOW_BENCH_RESULT_CACHE_NVME_MB`| `256`                                          |
//! | `FIRNFLOW_BENCH_OUT`                 | `bench/results/index_cache_<mode>.md`          |

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use firnflow_core::cache::NamespaceCache;
use firnflow_core::cachebackend::{
    CacheBackendCounters, CacheBackendCountersSnapshot, FoyerCacheBackend, FoyerCacheBackendConfig,
};
use firnflow_core::{
    CoreMetrics, NamespaceId, NamespaceManager, NamespaceService, QueryRequest, StorageRoot,
    UpsertRow,
};
use lance_core::cache::CacheBackend;
use lancedb::{ObjectStoreRegistry, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    CurrentFirn,
    WarmIndexCache,
    CacheDisabledCold,
}

impl Mode {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "current-firn" => Ok(Self::CurrentFirn),
            "warm-index-cache" => Ok(Self::WarmIndexCache),
            "cache-disabled-cold" => Ok(Self::CacheDisabledCold),
            other => anyhow::bail!(
                "unknown bench mode {other:?}: expected one of \
                 current-firn, warm-index-cache, cache-disabled-cold"
            ),
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::CurrentFirn => "current-firn",
            Self::WarmIndexCache => "warm-index-cache",
            Self::CacheDisabledCold => "cache-disabled-cold",
        }
    }
}

struct BenchConfig {
    mode: Mode,
    storage_root: StorageRoot,
    storage_options: HashMap<String, String>,
    namespace: String,
    dim: usize,
    rows: usize,
    measure_queries: usize,
    warmup_queries: usize,
    k: usize,
    nprobes: usize,
    result_cache_ram_bytes: usize,
    result_cache_nvme_bytes: usize,
    index_cache_ram_bytes: usize,
    index_cache_nvme_bytes: usize,
    out_path: PathBuf,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

impl BenchConfig {
    fn from_env() -> Result<Self> {
        let mode = Mode::parse(&env_or("FIRNFLOW_BENCH_INDEX_CACHE_MODE", "current-firn"))?;

        let uri = std::env::var("FIRNFLOW_STORAGE_URI")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty());
        let bucket = std::env::var("FIRNFLOW_S3_BUCKET")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty());
        let storage_root = match (uri, bucket) {
            (Some(uri), _) => StorageRoot::parse(&uri)
                .map_err(|e| anyhow::anyhow!("FIRNFLOW_STORAGE_URI ({uri:?}): {e}"))?,
            (None, Some(bucket)) => StorageRoot::s3_bucket(&bucket)
                .map_err(|e| anyhow::anyhow!("FIRNFLOW_S3_BUCKET ({bucket:?}): {e}"))?,
            (None, None) => anyhow::bail!(
                "set FIRNFLOW_STORAGE_URI=s3://bucket or the legacy FIRNFLOW_S3_BUCKET=bucket"
            ),
        };

        let mut opts = HashMap::new();
        if let Ok(v) = std::env::var("FIRNFLOW_S3_ENDPOINT") {
            opts.insert("aws_endpoint".into(), v);
            opts.insert("allow_http".into(), "true".into());
            opts.insert("aws_virtual_hosted_style_request".into(), "false".into());
        }
        if let Ok(v) = std::env::var("FIRNFLOW_S3_ACCESS_KEY") {
            opts.insert("aws_access_key_id".into(), v);
        }
        if let Ok(v) = std::env::var("FIRNFLOW_S3_SECRET_KEY") {
            opts.insert("aws_secret_access_key".into(), v);
        }
        opts.insert(
            "aws_region".into(),
            env_or("FIRNFLOW_S3_REGION", "us-east-1"),
        );

        let namespace = env_or("FIRNFLOW_BENCH_INDEX_CACHE_NS", "index-cache-bench");
        let dim: usize = env_or("FIRNFLOW_BENCH_DIM", "1536")
            .parse()
            .context("FIRNFLOW_BENCH_DIM")?;
        let rows: usize = env_or("FIRNFLOW_BENCH_ROWS", "1000000")
            .parse()
            .context("FIRNFLOW_BENCH_ROWS")?;
        let measure_queries: usize = env_or("FIRNFLOW_BENCH_QUERIES", "1000")
            .parse()
            .context("FIRNFLOW_BENCH_QUERIES")?;
        let warmup_queries: usize = env_or("FIRNFLOW_BENCH_WARMUP", "100")
            .parse()
            .context("FIRNFLOW_BENCH_WARMUP")?;
        let k: usize = env_or("FIRNFLOW_BENCH_K", "10")
            .parse()
            .context("FIRNFLOW_BENCH_K")?;
        let nprobes: usize = env_or("FIRNFLOW_BENCH_INDEX_NPROBES", "20")
            .parse()
            .context("FIRNFLOW_BENCH_INDEX_NPROBES")?;
        let result_cache_ram_mb: usize = env_or("FIRNFLOW_BENCH_RESULT_CACHE_RAM_MB", "16")
            .parse()
            .context("FIRNFLOW_BENCH_RESULT_CACHE_RAM_MB")?;
        let result_cache_nvme_mb: usize = env_or("FIRNFLOW_BENCH_RESULT_CACHE_NVME_MB", "256")
            .parse()
            .context("FIRNFLOW_BENCH_RESULT_CACHE_NVME_MB")?;
        let index_cache_ram_mb: usize = env_or("FIRNFLOW_BENCH_INDEX_CACHE_RAM_MB", "1024")
            .parse()
            .context("FIRNFLOW_BENCH_INDEX_CACHE_RAM_MB")?;
        let index_cache_nvme_mb: usize = env_or("FIRNFLOW_BENCH_INDEX_CACHE_NVME_MB", "10240")
            .parse()
            .context("FIRNFLOW_BENCH_INDEX_CACHE_NVME_MB")?;
        let default_out = format!("bench/results/index_cache_{}.md", mode.slug());
        let out_path = PathBuf::from(env_or("FIRNFLOW_BENCH_OUT", &default_out));

        Ok(Self {
            mode,
            storage_root,
            storage_options: opts,
            namespace,
            dim,
            rows,
            measure_queries,
            warmup_queries,
            k,
            nprobes,
            result_cache_ram_bytes: result_cache_ram_mb * 1024 * 1024,
            result_cache_nvme_bytes: result_cache_nvme_mb * 1024 * 1024,
            index_cache_ram_bytes: index_cache_ram_mb * 1024 * 1024,
            index_cache_nvme_bytes: index_cache_nvme_mb * 1024 * 1024,
            out_path,
        })
    }
}

/// Deterministic seed-vector. The same `i` produces the same
/// vector across modes so all three runs see the same dataset.
fn seed_vector(i: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| (((i * 7919 + j * 31) as f32) * 0.001).sin())
        .collect()
}

/// Deterministic query vector. Disjoint from `seed_vector` so
/// no query happens to be an exact match to a seeded row, which
/// would short-circuit the index traversal.
fn query_vector(i: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| (((i + 9_000_000) * 7919 + j * 31) as f32 * 0.001).sin())
        .collect()
}

struct Percentiles {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

fn percentiles(mut samples: Vec<Duration>) -> Percentiles {
    assert!(!samples.is_empty(), "no samples");
    samples.sort_unstable();
    let n = samples.len();
    Percentiles {
        p50: samples[n / 2],
        p95: samples[(n * 95) / 100],
        p99: samples[(n * 99) / 100],
        max: *samples.last().unwrap(),
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.2} ms", ms(d))
}

/// Sum `firnflow_s3_requests_total` across all `operation=` labels
/// for a given namespace. This counter is recorded by Firn at the
/// service boundary, not at Lance's object-store reads, so it
/// counts query / upsert / list calls rather than raw S3 GETs.
/// The plan calls this out: a foyer-warmed index does not change
/// this counter on its own. The number is reported anyway as a
/// sanity check on the bench shape.
fn sum_s3_requests(prom_text: &str, ns: &str) -> u64 {
    let ns_needle = format!(r#"namespace="{ns}""#);
    let mut total = 0_u64;
    for line in prom_text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !line.starts_with("firnflow_s3_requests_total{") {
            continue;
        }
        if !line.contains(&ns_needle) {
            continue;
        }
        if let Some((_, value)) = line.rsplit_once(char::is_whitespace) {
            if let Ok(v) = value.parse::<f64>() {
                total = total.saturating_add(v as u64);
            }
        }
    }
    total
}

/// Build the foyer-backed index cache and the lancedb Session
/// that carries it. Returns the session, the counters handle
/// for read-back at bench end, and the tempdir guard the foyer
/// NVMe tier lives in (kept alive for the duration of the run).
async fn build_index_cache_session(
    cfg: &BenchConfig,
) -> Result<(
    Arc<Session>,
    Arc<CacheBackendCounters>,
    Arc<FoyerCacheBackend>,
    tempfile::TempDir,
)> {
    let tmp = tempfile::tempdir()?;
    let counters = Arc::new(CacheBackendCounters::default());
    let backend = Arc::new(
        FoyerCacheBackend::new(
            FoyerCacheBackendConfig {
                memory_bytes: cfg.index_cache_ram_bytes,
                nvme_path: tmp.path(),
                nvme_bytes: cfg.index_cache_nvme_bytes,
            },
            Arc::clone(&counters),
        )
        .await
        .map_err(|e| anyhow::anyhow!("foyer cachebackend build: {e}"))?,
    );
    let backend_dyn: Arc<dyn CacheBackend> = backend.clone();
    let registry = Arc::new(ObjectStoreRegistry::default());
    // Metadata cache stays on foyer's moka default; only the
    // index cache is custom. Sized at 256 MiB to comfortably
    // hold table-open metadata even on the 1M-row corpus.
    let session = Arc::new(Session::with_index_cache_backend(
        backend_dyn,
        256 * 1024 * 1024,
        registry,
    ));
    Ok((session, counters, backend, tmp))
}

/// Seed the namespace with `cfg.rows` deterministic vectors,
/// batching upserts at 10k rows each so a single Arrow batch
/// never balloons the bench host. Returns wall-clock seed time.
async fn seed_namespace(
    service: &NamespaceService,
    ns: &NamespaceId,
    rows: usize,
    dim: usize,
) -> Result<Duration> {
    const BATCH: usize = 10_000;
    let start = Instant::now();
    let mut offset = 0;
    while offset < rows {
        let end = (offset + BATCH).min(rows);
        let batch: Vec<UpsertRow> = (offset..end)
            .map(|i| (i as u64, seed_vector(i, dim)).into())
            .collect();
        service.upsert(ns, batch).await?;
        offset = end;
        if offset % (BATCH * 10) == 0 || offset == rows {
            println!("  seeded {offset}/{rows} rows");
        }
    }
    Ok(start.elapsed())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = BenchConfig::from_env()?;
    println!(
        "mode={}, dim={}, rows={}, queries={} (+{} warmup), k={}, nprobes={}",
        cfg.mode.slug(),
        cfg.dim,
        cfg.rows,
        cfg.measure_queries,
        cfg.warmup_queries,
        cfg.k,
        cfg.nprobes,
    );
    println!(
        "namespace={}, storage_root={}",
        cfg.namespace, cfg.storage_root
    );

    // Build the index-cache session for warm-index-cache mode.
    // For other modes the session stays None and the manager
    // behaves exactly as production does today.
    let index_cache_state = if cfg.mode == Mode::WarmIndexCache {
        Some(build_index_cache_session(&cfg).await?)
    } else {
        None
    };

    let metrics = Arc::new(CoreMetrics::new()?);
    let mut manager = NamespaceManager::new(
        cfg.storage_root.clone(),
        cfg.storage_options.clone(),
        Arc::clone(&metrics),
    );
    if let Some((session, _, _, _)) = &index_cache_state {
        manager = manager.with_session(Arc::clone(session));
    }
    let manager = Arc::new(manager);

    let result_cache_tmp = tempfile::tempdir()?;
    let result_cache = Arc::new(
        NamespaceCache::new(
            cfg.result_cache_ram_bytes,
            result_cache_tmp.path(),
            cfg.result_cache_nvme_bytes,
            Arc::clone(&metrics),
        )
        .await
        .map_err(|e| anyhow::anyhow!("result cache build: {e}"))?,
    );
    let service = NamespaceService::new(
        Arc::clone(&manager),
        Arc::clone(&result_cache),
        Arc::clone(&metrics),
    );

    let ns = NamespaceId::new(&cfg.namespace)?;

    // Eagerly probe the namespace so the manager's in-memory
    // schema cache is populated from the persisted Lance table
    // (if any). `dim_for(&ns)` is otherwise None on every fresh
    // process and the reuse branch below never triggers.
    //
    // Three outcomes:
    //   - namespace does not exist: query returns an empty
    //     result set and `dim_for` stays `None`.
    //   - namespace exists with `cfg.dim`: `dim_for` is set,
    //     query returns hits (which we discard).
    //   - namespace exists with a different dim: query fails
    //     with an `InvalidRequest`, but the schema-read
    //     side-effect already set `dim_for` to the persisted
    //     dim, so the match below produces a clean error.
    let _ = manager
        .query(&ns, vec![0.0; cfg.dim], None, 1, None, None)
        .await;

    // Seed if the namespace doesn't already exist with the right
    // dim. Re-using a pre-seeded namespace across modes is the
    // whole point of stable naming: the 1M-row write happens
    // once and three mode runs read from it.
    let seed_elapsed = match manager.dim_for(&ns) {
        Some(d) if d == cfg.dim => {
            println!("namespace exists with dim={d}, skipping seed + index build");
            Duration::ZERO
        }
        Some(d) => {
            anyhow::bail!(
                "namespace `{}` already exists with dim={d}, but the bench was \
                 invoked with FIRNFLOW_BENCH_DIM={}. Pick a different \
                 FIRNFLOW_BENCH_INDEX_CACHE_NS or matching dim.",
                cfg.namespace,
                cfg.dim
            );
        }
        None => {
            // Either fresh namespace or a dim mismatch. The
            // resolve_schema_info call inside upsert handles
            // both cases.
            println!("seeding {} rows ({} dim)...", cfg.rows, cfg.dim);
            let elapsed = seed_namespace(&service, &ns, cfg.rows, cfg.dim).await?;
            println!(
                "  seeded in {:.1}s ({:.0} rows/s)",
                elapsed.as_secs_f64(),
                cfg.rows as f64 / elapsed.as_secs_f64()
            );

            // IVF_PQ parameters: num_partitions ~= sqrt(rows),
            // num_sub_vectors = dim/16. The same defaults the
            // existing harness picks for cold/warm comparison.
            let num_partitions = (cfg.rows as f64).sqrt() as u32;
            let num_sub_vectors = (cfg.dim / 16).max(1) as u32;
            println!(
                "building IVF_PQ: partitions={num_partitions}, sub_vectors={num_sub_vectors}..."
            );
            let idx_start = Instant::now();
            manager
                .create_index(&ns, Some(num_partitions), Some(num_sub_vectors))
                .await?;
            println!("  index built in {:.1}s", idx_start.elapsed().as_secs_f64());
            elapsed
        }
    };

    // Generate every query up front so latency timing covers
    // only the query call itself, not vector construction.
    //
    // Warmup queries are only used when the warm-index-cache
    // mode is active. Running them on the other modes would
    // warm Lance's default in-memory session cache (Moka-backed
    // when no custom `CacheBackend` is installed), which would
    // contaminate the baseline. The vector budget shrinks
    // accordingly for the other modes.
    let warmup_count = if cfg.mode == Mode::WarmIndexCache {
        cfg.warmup_queries
    } else {
        0
    };
    let total_queries = warmup_count + cfg.measure_queries;
    println!("generating {total_queries} query vectors...");
    let queries: Vec<Vec<f32>> = (0..total_queries)
        .map(|i| query_vector(i, cfg.dim))
        .collect();

    if warmup_count > 0 {
        println!("warmup: {warmup_count} queries (warm-index-cache only)...");
        for v in &queries[..warmup_count] {
            let _ = manager
                .query(&ns, v.clone(), None, cfg.k, Some(cfg.nprobes), None)
                .await?;
        }
    }

    // Counter + metric snapshot before the measurement window.
    let counters_before = index_cache_state
        .as_ref()
        .map(|(_, c, _, _)| c.snapshot())
        .unwrap_or_default();
    let metrics_before = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let s3_before = sum_s3_requests(&metrics_before, &cfg.namespace);

    // Measurement loop. Latency clock per query, no other work
    // inside the timed region.
    println!("measure: {} queries...", cfg.measure_queries);
    let mut samples = Vec::with_capacity(cfg.measure_queries);
    let measure_start = Instant::now();
    for v in &queries[warmup_count..] {
        let start = Instant::now();
        match cfg.mode {
            Mode::CacheDisabledCold => {
                let _ = manager
                    .query(&ns, v.clone(), None, cfg.k, Some(cfg.nprobes), None)
                    .await?;
            }
            Mode::CurrentFirn | Mode::WarmIndexCache => {
                let _ = service
                    .query(
                        &ns,
                        &QueryRequest {
                            vector: v.clone(),
                            vectors: None,
                            k: cfg.k,
                            nprobes: Some(cfg.nprobes),
                            text: None,
                        },
                    )
                    .await?;
            }
        }
        samples.push(start.elapsed());
    }
    let measure_elapsed = measure_start.elapsed();
    println!(
        "  measure completed in {:.1}s ({:.0} q/s)",
        measure_elapsed.as_secs_f64(),
        cfg.measure_queries as f64 / measure_elapsed.as_secs_f64()
    );

    let counters_after = index_cache_state
        .as_ref()
        .map(|(_, c, _, _)| c.snapshot())
        .unwrap_or_default();
    let metrics_after = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let s3_after = sum_s3_requests(&metrics_after, &cfg.namespace);

    let p = percentiles(samples);

    // Report.
    let date = chrono_today();
    let counter_delta = CacheBackendCountersSnapshot {
        get: counters_after.get.saturating_sub(counters_before.get),
        insert: counters_after.insert.saturating_sub(counters_before.insert),
        get_or_insert: counters_after
            .get_or_insert
            .saturating_sub(counters_before.get_or_insert),
        invalidate_prefix: counters_after
            .invalidate_prefix
            .saturating_sub(counters_before.invalidate_prefix),
        clear: counters_after.clear.saturating_sub(counters_before.clear),
    };

    let foyer_section = if cfg.mode == Mode::WarmIndexCache {
        format!(
            "## Cache-backend traffic (measure window)\n\n\
             | counter | delta |\n\
             | --- | ---: |\n\
             | `get` | {} |\n\
             | `insert` | {} |\n\
             | `get_or_insert` | {} |\n\n\
             Hit-rate signal: `insert` deltas approach zero as Lance \
             finds its index state already cached. A non-trivial \
             `insert` count after a warmup pass would mean Lance is \
             still opening fresh state under load, which is worth a \
             second look.\n\n",
            counter_delta.get, counter_delta.insert, counter_delta.get_or_insert
        )
    } else {
        String::new()
    };

    let storage_mb = (cfg.rows as f64 * cfg.dim as f64 * 4.0) / (1024.0 * 1024.0);

    let report = format!(
        "# Index cache bench: {mode}\n\
\n\
- **Date**: {date}\n\
- **Mode**: `{mode}`\n\
- **Backend**: see `FIRNFLOW_STORAGE_URI` / `FIRNFLOW_S3_ENDPOINT`\n\
- **Namespace**: `{ns}` (re-used across modes; seed is amortised)\n\
- **Workload**: dim={dim}, rows={rows} (~{storage_mb:.0} MB raw), \
queries={queries} (+{warmup} warmup), k={k}, nprobes={nprobes}\n\
- **Seed**: {seed_secs}\n\
- **Index cache (mode=warm-index-cache only)**: \
RAM={icr}MB, NVMe={icv}MB\n\
- **Result cache**: RAM={rcr}MB, NVMe={rcv}MB\n\
\n\
## Latency (1000-query window)\n\
\n\
| p50 | p95 | p99 | max |\n\
| ---: | ---: | ---: | ---: |\n\
| {p50} | {p95} | {p99} | {pmax} |\n\
\n\
{foyer_section}\
## Firn-level S3 requests (delta)\n\
\n\
| counter | delta |\n\
| --- | ---: |\n\
| `firnflow_s3_requests_total` for this namespace | {s3_delta} |\n\
\n\
Caveat: this counter is recorded at Firn's service boundary, not \
at Lance's internal object-store reads. It tracks how many queries \
were issued, not how many S3 GETs they triggered. A foyer-warmed \
index cache reduces Lance's internal reads without changing this \
number. The right S3-reduction signal for the warm-index-cache run \
is the cache-backend `insert` delta above (a low `insert` count on a \
1000-query warm window implies Lance found most of what it needed \
without re-fetching it from S3).\n\
\n\
## Notes\n\
\n\
- The same {queries} measure-window query vectors are used \
across modes (stable seed). Each is freshly generated and disjoint \
from the seeded corpus, so every query is novel and the service-\
level result cache never hits.\n\
- The warmup pass ({warmup} queries on the warm-index-cache mode, \
zero on the other modes) runs against the manager directly to \
populate the foyer index cache before the measure window. Other \
modes deliberately skip the warmup so the baseline does not warm \
Lance's default in-memory session cache, which would contaminate \
the comparison.\n\
- Cache-backend invocation counters are aggregate across Lance \
type names and key prefixes. A per-(type_name, prefix) breakdown \
is a follow-up; the aggregate is sufficient to read the warm-vs-\
cold signal.\n\
",
        mode = cfg.mode.slug(),
        date = date,
        ns = cfg.namespace,
        dim = cfg.dim,
        rows = cfg.rows,
        storage_mb = storage_mb,
        queries = cfg.measure_queries,
        warmup = cfg.warmup_queries,
        k = cfg.k,
        nprobes = cfg.nprobes,
        seed_secs = if seed_elapsed.is_zero() {
            "skipped (namespace reused)".to_string()
        } else {
            format!("{:.1}s", seed_elapsed.as_secs_f64())
        },
        icr = cfg.index_cache_ram_bytes / (1024 * 1024),
        icv = cfg.index_cache_nvme_bytes / (1024 * 1024),
        rcr = cfg.result_cache_ram_bytes / (1024 * 1024),
        rcv = cfg.result_cache_nvme_bytes / (1024 * 1024),
        p50 = fmt_ms(p.p50),
        p95 = fmt_ms(p.p95),
        p99 = fmt_ms(p.p99),
        pmax = fmt_ms(p.max),
        foyer_section = foyer_section,
        s3_delta = s3_after.saturating_sub(s3_before),
    );

    if let Some(parent) = cfg.out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating output directory {}", parent.display()))?;
    }
    fs::write(&cfg.out_path, &report)
        .with_context(|| format!("writing {}", cfg.out_path.display()))?;
    println!("\nwrote {}", cfg.out_path.display());
    println!(
        "latency: p50={} p95={} p99={} max={}",
        fmt_ms(p.p50),
        fmt_ms(p.p95),
        fmt_ms(p.p99),
        fmt_ms(p.max)
    );
    if cfg.mode == Mode::WarmIndexCache {
        println!(
            "cache-backend delta: get +{} insert +{} get_or_insert +{}",
            counter_delta.get, counter_delta.insert, counter_delta.get_or_insert
        );
    }
    println!(
        "firnflow_s3_requests_total delta: {}",
        s3_after.saturating_sub(s3_before)
    );

    Ok(())
}

fn chrono_today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs / 86_400;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}
