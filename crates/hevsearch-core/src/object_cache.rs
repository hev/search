//! Local-NVMe byte-range cache for Lance object-store reads (GitHub issue #51).
//!
//! hev search's existing result cache (`crate::cache`) only helps *exact repeated* queries. Profiling on
//! real in-region S3 showed ~95% of cold *and* warm query latency is object-store I/O — a cold
//! IVF_PQ query issues ~140 small GETs (mostly index), bound by request count, not bytes. This
//! module caches the byte ranges Lance reads from object storage on local NVMe, so repeated /
//! warm / non-identical queries over the same dataset are served locally instead of paying S3
//! round-trips (a local-SSD cache in front of object storage; the approach #51 asks for).
//!
//! ## Integration
//! Lance reads through `object_store::ObjectStore`. We wrap that store with [`CachingObjectStore`]
//! by registering a [`CachingProvider`] (a `lance_io` `ObjectStoreProvider`) for the cloud schemes
//! in a custom [`ObjectStoreRegistry`], wrapped in a [`lance::session::Session`] passed to lancedb's
//! `connect().session(..)`. No fork of lance/lancedb — `ObjectStore.inner` is public, so the
//! provider builds the real store and swaps in the cache.
//!
//! ## Correctness (why no write/delete invalidation is needed)
//! We cache **only immutable, uniquely-named (write-once) objects** — Lance data fragments (`data/<uuid>.lance`)
//! and index files (`_indices/<uuid>/...`). These are write-once and uniquely named, so:
//!   * a namespace delete + recreate under the same path produces *new* UUIDs → new cache keys; old
//!     entries are simply unreferenced orphans that are never served (and are reclaimed by LRU
//!     eviction). NB `NamespaceManager::delete` deletes via a *separate* (uncached) object store, so
//!     the cache never even sees those deletes — write-once unique naming is what keeps it correct.
//!   * mutable / version-numbered paths (manifests, `_versions/`, `_transactions/`, `_latest`, the
//!     "latest version" pointer) and `HEAD` requests are **never cached** — they pass straight
//!     through. This is what makes write/delete invalidation unnecessary for correctness.
//!
//! Invalidation is therefore just capacity eviction, not coherence.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use lru::LruCache;
use prometheus::{IntCounter, Opts, Registry};
use tokio::fs;

use object_store::path::Path as OPath;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OResult,
};

use lance::Result as LanceResult;
use lance_io::object_store::providers::ObjectStoreProvider;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreRegistry,
};

/// Default per-entry buffering limit (256 MiB). A single cacheable read above this is streamed
/// straight through rather than buffered + cached, so a whole-object read of a large fragment
/// cannot spike RAM by the full object size.
pub const DEFAULT_MAX_ENTRY_BYTES: u64 = 256 * 1024 * 1024;

/// Configuration for the object cache, sourced from operator env vars.
#[derive(Clone, Debug)]
pub struct ObjectCacheConfig {
    /// Root directory on local NVMe for cached byte ranges.
    pub dir: PathBuf,
    /// Total on-disk capacity in bytes; LRU eviction keeps usage under this.
    pub capacity_bytes: u64,
    /// Largest single read that will be buffered and cached. Reads above this stream straight
    /// through uncached, bounding the RAM a single miss can use.
    pub max_entry_bytes: u64,
    /// Cloud URI schemes whose stores should be cache-wrapped.
    pub schemes: Vec<String>,
}

impl ObjectCacheConfig {
    /// Build a config for the given cache directory and capacity, wrapping the
    /// default cloud schemes (`s3`, `s3+ddb`, `gs`).
    pub fn new(dir: PathBuf, capacity_bytes: u64) -> Self {
        Self {
            dir,
            capacity_bytes,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            schemes: vec!["s3".into(), "s3+ddb".into(), "gs".into()],
        }
    }
}

/// Process-wide counters for the object cache (shared across per-bucket stores).
///
/// Backed by Prometheus `IntCounter`s. When built via [`ObjectCacheMetrics::register`] the
/// counters are registered with the core registry, so they surface at `/metrics` and can be
/// scraped into Grafana. The counters are global (not per-namespace): the cache sits at the
/// object-store layer, below the namespace abstraction, so it has no clean namespace label.
#[derive(Clone, Debug)]
pub struct ObjectCacheMetrics {
    /// Reads that went to the inner (object storage) store — misses + uncacheable passthroughs.
    pub inner_gets: IntCounter,
    /// Reads served from the local cache.
    pub hits: IntCounter,
    /// Cacheable reads that missed, were fetched from object storage, and were written to the
    /// local cache. Counted only after a successful cache admission.
    pub misses: IntCounter,
    /// Bytes fetched from object storage.
    pub s3_bytes: IntCounter,
    /// Cache entries evicted for capacity.
    pub evictions: IntCounter,
}

impl ObjectCacheMetrics {
    /// Create the object-cache counters and register them with `registry`.
    ///
    /// Increments on the returned handle are visible in the registry's `/metrics` render.
    pub fn register(registry: &Registry) -> prometheus::Result<Self> {
        let mk = |name: &str, help: &str| -> prometheus::Result<IntCounter> {
            let counter = IntCounter::with_opts(Opts::new(name, help))?;
            registry.register(Box::new(counter.clone()))?;
            Ok(counter)
        };
        Ok(Self {
            inner_gets: mk(
                "hevsearch_object_cache_inner_gets_total",
                "Object-cache reads forwarded to object storage (misses + uncacheable passthroughs)",
            )?,
            hits: mk(
                "hevsearch_object_cache_hits_total",
                "Object-cache reads served from the local disk cache",
            )?,
            misses: mk(
                "hevsearch_object_cache_misses_total",
                "Object-cache cacheable reads that missed and were fetched + cached",
            )?,
            s3_bytes: mk(
                "hevsearch_object_cache_s3_bytes_total",
                "Bytes fetched from object storage by the object cache",
            )?,
            evictions: mk(
                "hevsearch_object_cache_evictions_total",
                "Object-cache entries evicted for capacity",
            )?,
        })
    }

    /// Standalone, unregistered counters — for tests and the disabled path.
    pub fn unregistered() -> Self {
        let mk = |name: &str| IntCounter::with_opts(Opts::new(name, name)).expect("counter opts");
        Self {
            inner_gets: mk("hevsearch_object_cache_inner_gets_total"),
            hits: mk("hevsearch_object_cache_hits_total"),
            misses: mk("hevsearch_object_cache_misses_total"),
            s3_bytes: mk("hevsearch_object_cache_s3_bytes_total"),
            evictions: mk("hevsearch_object_cache_evictions_total"),
        }
    }

    /// Return a point-in-time `(inner_gets, hits, misses, s3_bytes, evictions)` snapshot.
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.inner_gets.get(),
            self.hits.get(),
            self.misses.get(),
            self.s3_bytes.get(),
            self.evictions.get(),
        )
    }
}

impl Default for ObjectCacheMetrics {
    fn default() -> Self {
        Self::unregistered()
    }
}

// ---------------------------------------------------------------------------
// Byte-bounded LRU (O(1) touch/insert/evict via the `lru` crate; capacity in bytes)
// ---------------------------------------------------------------------------

struct ByteLru {
    cap: u64,
    used: u64,
    map: LruCache<String, u64>, // key -> entry size in bytes; count-unbounded, byte-bounded here
}

impl ByteLru {
    fn new(cap: u64) -> Self {
        Self {
            cap,
            used: 0,
            map: LruCache::unbounded(),
        }
    }
    /// Mark `key` most-recently-used. O(1).
    fn touch(&mut self, key: &str) {
        let _ = self.map.get(key);
    }
    /// Admit a new entry; returns keys whose files must be deleted to honour the byte cap.
    fn admit(&mut self, key: String, size: u64) -> Vec<String> {
        if self.map.contains(&key) {
            let _ = self.map.get(&key);
            return Vec::new();
        }
        self.map.put(key, size);
        self.used = self.used.saturating_add(size);
        let mut evicted = Vec::new();
        while self.used > self.cap {
            match self.map.pop_lru() {
                Some((k, sz)) => {
                    self.used = self.used.saturating_sub(sz);
                    evicted.push(k);
                }
                None => break,
            }
        }
        evicted
    }
}

// ---------------------------------------------------------------------------
// CachingObjectStore
// ---------------------------------------------------------------------------

/// Read-through byte-range cache over an inner object store. One per bucket
/// (lance's registry reuses it across namespaces in the same bucket).
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
    /// Largest single read that will be buffered + cached; larger reads stream through uncached.
    /// Clamped at construction to the cache capacity (an entry bigger than the whole cache could
    /// never be retained).
    max_entry_bytes: u64,
    metrics: Arc<ObjectCacheMetrics>,
    lru: Mutex<ByteLru>,
    /// single-flight: one in-flight fetch per cache key; entries are removed once the
    /// fetch completes and no other waiter holds the gate (see `release_inflight`).
    inflight: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl std::fmt::Debug for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({:?})", self.inner)
    }
}
impl std::fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

fn hash_key(parts: &[&str]) -> String {
    // DefaultHasher is deterministic across processes (fixed seed) → stable keys across restarts.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut h);
        0u8.hash(&mut h);
    }
    format!("{:016x}", h.finish())
}

fn sz_path(p: &FsPath) -> PathBuf {
    p.with_extension("sz")
}

impl CachingObjectStore {
    /// Wrap `inner` with a read-through byte-range cache stored under `dir`, bounded to
    /// `capacity_bytes` (LRU eviction), recording into `metrics`. The cache directory is scanned on
    /// construction so pre-existing entries count toward the byte cap (durable across restarts and
    /// store re-creation; over-cap files are evicted immediately).
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        dir: PathBuf,
        capacity_bytes: u64,
        max_entry_bytes: u64,
        metrics: Arc<ObjectCacheMetrics>,
    ) -> Self {
        let _ = std::fs::create_dir_all(&dir);
        let mut lru = ByteLru::new(capacity_bytes);

        // Startup scan: admit existing cache files (oldest first) so capacity is honoured across
        // restarts / dropped-and-recreated stores. `.sz` sidecars are auxiliary; `.tmp` files are
        // stale partial writes from a crash/failed rename — they aren't counted toward the cap, so
        // delete them now rather than leak uncounted (possibly large) bytes.
        let mut found: Vec<(String, u64, SystemTime)> = Vec::new();
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for ent in rd.flatten() {
                let name = match ent.file_name().to_str() {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                if name.ends_with(".tmp") {
                    let _ = std::fs::remove_file(ent.path());
                    continue;
                }
                if name.ends_with(".sz") {
                    continue;
                }
                if let Ok(meta) = ent.metadata() {
                    if meta.is_file() {
                        let mt = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                        found.push((name, meta.len(), mt));
                    }
                }
            }
        }
        found.sort_by_key(|(_, _, mt)| *mt);
        for (name, size, _) in found {
            for k in lru.admit(name, size) {
                let _ = std::fs::remove_file(dir.join(&k));
                let _ = std::fs::remove_file(sz_path(&dir.join(&k)));
            }
        }

        Self {
            inner,
            dir,
            // An entry larger than the whole cache could never be retained (the LRU would evict it
            // immediately), so never buffer one: the effective limit is the smaller of the two.
            max_entry_bytes: max_entry_bytes.min(capacity_bytes),
            metrics,
            lru: Mutex::new(lru),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    fn key_path(&self, location: &OPath, range: Option<(u64, u64)>) -> (String, PathBuf) {
        let r = match range {
            Some((s, e)) => format!("{s}-{e}"),
            None => "whole".to_string(),
        };
        let key = hash_key(&[location.as_ref(), &r]);
        let path = self.dir.join(&key);
        (key, path)
    }

    fn inflight_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.inflight
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Remove the single-flight entry once no other waiter holds the gate. Both this and
    /// `inflight_lock` hold the map lock, so the strong-count check is race-free (no new waiter can
    /// clone the gate while we hold the lock). `gate` strong count is 2 (map + our clone) when we
    /// are the last holder.
    fn release_inflight(&self, key: &str, gate: &Arc<tokio::sync::Mutex<()>>) {
        let mut m = self.inflight.lock().unwrap();
        if Arc::strong_count(gate) <= 2 {
            m.remove(key);
        }
    }

    async fn admit_file(&self, key: String, size: u64) {
        let evicted = self.lru.lock().unwrap().admit(key, size);
        for k in evicted {
            let p = self.dir.join(&k);
            let _ = fs::remove_file(&p).await;
            let _ = fs::remove_file(sz_path(&p)).await;
            self.metrics.evictions.inc();
        }
    }

    /// Try to serve `(location, range)` from disk. Returns the result on a hit.
    async fn disk_hit(
        &self,
        key: &str,
        path: &FsPath,
        location: &OPath,
        range: Option<(u64, u64)>,
    ) -> Option<GetResult> {
        // Require the size sidecar first. A payload without it (a crash between rename and sidecar
        // write, or a failed sidecar write) would otherwise be served with a wrong object size on a
        // range read, so treat it as a miss — and read the small sidecar before the payload so we
        // don't read a potentially large orphan just to reject it.
        let obj_size = fs::read_to_string(sz_path(path))
            .await
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())?;
        let bytes = fs::read(path).await.ok()?;
        self.metrics.hits.inc();
        self.lru.lock().unwrap().touch(key);
        Some(build_result(location.clone(), bytes, range, Some(obj_size)))
    }
}

/// Cache only immutable, uniquely-named (write-once) objects (data fragments + index files). Everything else
/// — mutable / version-numbered metadata and the latest-version pointer — is never cached.
fn should_cache(location: &OPath) -> bool {
    let p = location.as_ref();
    if p.contains("_versions")
        || p.contains("_transactions")
        || p.contains("_latest")
        || p.ends_with(".manifest")
    {
        return false;
    }
    p.contains("_indices") || p.contains("/data/") || p.ends_with(".lance")
}

/// Whether a `GetOptions` is a plain (cacheable) read: not a HEAD, no conditional headers, no
/// explicit object version, and a whole-object or bounded-range read (offset/suffix pass through
/// uncached). The cache key is `(path, range)` only, so anything that changes the bytes a given
/// `(path, range)` resolves to — a HEAD (metadata, no body), a conditional, or a `version` — must
/// not be cached, or it would collide with a plain read of the same key.
fn plain_read(o: &GetOptions) -> bool {
    if o.head
        || o.version.is_some()
        || o.if_match.is_some()
        || o.if_none_match.is_some()
        || o.if_modified_since.is_some()
        || o.if_unmodified_since.is_some()
    {
        return false;
    }
    matches!(o.range, None | Some(GetRange::Bounded(_)))
}

fn bounded_or_whole(o: &GetOptions) -> Option<(u64, u64)> {
    match &o.range {
        Some(GetRange::Bounded(r)) => Some((r.start, r.end)),
        _ => None,
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn get_opts(&self, location: &OPath, options: GetOptions) -> OResult<GetResult> {
        if !should_cache(location) || !plain_read(&options) {
            self.metrics.inner_gets.inc();
            return self.inner.get_opts(location, options).await;
        }
        let range = bounded_or_whole(&options);
        let (key, path) = self.key_path(location, range);

        // fast path
        if let Some(hit) = self.disk_hit(&key, &path, location, range).await {
            return Ok(hit);
        }

        // single-flight: serialize concurrent identical misses, then self-clean.
        let gate = self.inflight_lock(&key);
        let _g = gate.lock().await;
        let out = if let Some(hit) = self.disk_hit(&key, &path, location, range).await {
            Ok(hit)
        } else {
            self.fetch_and_cache(location, options, &key, &path).await
        };
        drop(_g);
        self.release_inflight(&key, &gate);
        out
    }

    async fn put_opts(&self, l: &OPath, p: PutPayload, o: PutOptions) -> OResult<PutResult> {
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &OPath,
        o: PutMultipartOptions,
    ) -> OResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(l, o).await
    }
    async fn delete(&self, l: &OPath) -> OResult<()> {
        self.inner.delete(l).await
    }
    fn list(&self, prefix: Option<&OPath>) -> BoxStream<'static, OResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&OPath>) -> OResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &OPath, to: &OPath) -> OResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &OPath, to: &OPath) -> OResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl CachingObjectStore {
    async fn fetch_and_cache(
        &self,
        location: &OPath,
        options: GetOptions,
        key: &str,
        path: &FsPath,
    ) -> OResult<GetResult> {
        self.metrics.inner_gets.inc();
        let res = self.inner.get_opts(location, options).await?;

        // Bound the buffered read. A whole-object (or large-range) read of a big fragment would
        // otherwise spike RAM by the full size: above the per-entry limit we stream the inner
        // result straight back to Lance without buffering or caching it.
        let returned_len = res.range.end.saturating_sub(res.range.start);
        if returned_len > self.max_entry_bytes {
            return Ok(res);
        }

        let meta = res.meta.clone();
        let attrs = res.attributes.clone();
        let actual = res.range.clone();
        let bytes = res.bytes().await?;
        self.metrics.s3_bytes.inc_by(bytes.len() as u64);

        // Count a miss only once the bytes are durably cached, so the metric means
        // "fetched and cached" — a failed disk write (or an over-limit passthrough above) is not
        // counted as a miss and so never inflates the hit-rate denominator. Always remove the temp
        // file on a failed write/rename so a partial write can't leak uncounted bytes past the cap.
        let tmp = path.with_extension("tmp");
        let cached = match fs::write(&tmp, &bytes).await {
            Ok(()) => match fs::rename(&tmp, path).await {
                Ok(()) => true,
                Err(_) => {
                    let _ = fs::remove_file(&tmp).await;
                    false
                }
            },
            Err(_) => {
                let _ = fs::remove_file(&tmp).await;
                false
            }
        };
        if cached {
            // Retain the payload only alongside its size sidecar — a payload without it is unservable
            // (see `disk_hit`). If the sidecar write fails, drop the payload so it doesn't sit on
            // disk counting toward the cap while never being served.
            if fs::write(sz_path(path), meta.size.to_string())
                .await
                .is_ok()
            {
                self.admit_file(key.to_string(), bytes.len() as u64).await;
                self.metrics.misses.inc();
            } else {
                let _ = fs::remove_file(path).await;
            }
        }
        Ok(GetResult {
            payload: one_chunk(bytes),
            meta,
            range: actual,
            attributes: attrs,
        })
    }
}

fn one_chunk(b: Bytes) -> GetResultPayload {
    GetResultPayload::Stream(stream::once(async move { Ok(b) }).boxed())
}

fn build_result(
    location: OPath,
    bytes: Vec<u8>,
    range: Option<(u64, u64)>,
    obj_size: Option<u64>,
) -> GetResult {
    let bytes = Bytes::from(bytes);
    let len = bytes.len() as u64;
    let start = range.map(|(s, _)| s).unwrap_or(0);
    let meta = ObjectMeta {
        location,
        last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        size: obj_size.unwrap_or(len),
        e_tag: None,
        version: None,
    };
    GetResult {
        payload: one_chunk(bytes),
        meta,
        range: start..start + len,
        attributes: Attributes::default(),
    }
}

// ---------------------------------------------------------------------------
// CachingProvider — wraps a real provider's store with the cache
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CachingProvider {
    inner: Arc<dyn ObjectStoreProvider>,
    cfg: ObjectCacheConfig,
    metrics: Arc<ObjectCacheMetrics>,
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[async_trait]
impl ObjectStoreProvider for CachingProvider {
    async fn new_store(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> LanceResult<LanceObjectStore> {
        let mut store = self.inner.new_store(base_path.clone(), params).await?;
        let prefix = self
            .inner
            .calculate_object_store_prefix(&base_path, params.storage_options())?;
        let cache = CachingObjectStore::new(
            store.inner.clone(),
            self.cfg.dir.join(sanitize(&prefix)),
            self.cfg.capacity_bytes,
            self.cfg.max_entry_bytes,
            self.metrics.clone(),
        );
        store.inner = Arc::new(cache);
        Ok(store)
    }

    fn extract_path(&self, url: &url::Url) -> LanceResult<OPath> {
        self.inner.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &url::Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> LanceResult<String> {
        self.inner
            .calculate_object_store_prefix(url, storage_options)
    }
}

// ---------------------------------------------------------------------------
// Session builder — what manager.rs hands to lancedb connect().session(..)
// ---------------------------------------------------------------------------

/// Build a lance [`Session`](lance::session::Session) whose object-store registry wraps cloud
/// stores with the byte-range cache, recording into `metrics`. Pass the returned session to
/// `lancedb::connect(..).session(session)`. Hand in `CoreMetrics::object_cache()` so the counters
/// surface at `/metrics`, or [`ObjectCacheMetrics::unregistered`] when you don't need them scraped.
pub fn build_cached_session(
    cfg: &ObjectCacheConfig,
    metrics: Arc<ObjectCacheMetrics>,
) -> Arc<lance::session::Session> {
    let registry = ObjectStoreRegistry::default();
    for scheme in &cfg.schemes {
        if let Some(inner) = registry.get_provider(scheme) {
            registry.insert(
                scheme,
                Arc::new(CachingProvider {
                    inner,
                    cfg: cfg.clone(),
                    metrics: metrics.clone(),
                }),
            );
        }
    }
    let session = lance::session::Session::new(
        lance::dataset::DEFAULT_INDEX_CACHE_SIZE,
        lance::dataset::DEFAULT_METADATA_CACHE_SIZE,
        Arc::new(registry),
    );
    Arc::new(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn cache(
        dir: &FsPath,
        inner: Arc<dyn ObjectStore>,
    ) -> (CachingObjectStore, Arc<ObjectCacheMetrics>) {
        let m = Arc::new(ObjectCacheMetrics::default());
        (
            CachingObjectStore::new(
                inner,
                dir.to_path_buf(),
                64 * 1024 * 1024,
                u64::MAX,
                m.clone(),
            ),
            m,
        )
    }

    #[tokio::test]
    async fn caches_immutable_range_and_serves_repeat_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![7u8; 1024 * 1024]))
            .await
            .unwrap();
        let (c, m) = cache(tmp.path(), inner);

        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..65_536)),
            ..Default::default()
        };
        let a = c
            .get_opts(&loc, opts.clone())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let b = c.get_opts(&loc, opts).await.unwrap().bytes().await.unwrap();
        assert_eq!(a, b);
        let (inner_gets, hits, misses, _, _) = m.snapshot();
        assert_eq!(
            (inner_gets, hits, misses),
            (1, 1, 1),
            "miss then hit, inner touched once"
        );
    }

    #[tokio::test]
    async fn metrics_register_and_increment_via_registry() {
        use prometheus::{Encoder, TextEncoder};

        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![3u8; 4096]))
            .await
            .unwrap();

        // Counters registered with a real registry (mirrors CoreMetrics wiring).
        let registry = Registry::new();
        let m = Arc::new(ObjectCacheMetrics::register(&registry).unwrap());
        let c = CachingObjectStore::new(
            inner,
            tmp.path().to_path_buf(),
            64 * 1024 * 1024,
            u64::MAX,
            m.clone(),
        );

        // Miss then hit on the same immutable range.
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..4096)),
            ..Default::default()
        };
        let _ = c
            .get_opts(&loc, opts.clone())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let _ = c.get_opts(&loc, opts).await.unwrap().bytes().await.unwrap();

        // Counters moved on the handle...
        let (inner_gets, hits, misses, s3_bytes, _) = m.snapshot();
        assert_eq!((inner_gets, hits, misses), (1, 1, 1));
        assert_eq!(s3_bytes, 4096);

        // ...and are exposed in the registry's `/metrics` render with the same values.
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            text.contains("hevsearch_object_cache_hits_total 1"),
            "hits not exposed:\n{text}"
        );
        assert!(text.contains("hevsearch_object_cache_misses_total 1"));
        assert!(text.contains("hevsearch_object_cache_inner_gets_total 1"));
        assert!(text.contains("hevsearch_object_cache_s3_bytes_total 4096"));
    }

    #[tokio::test]
    async fn reads_over_the_entry_limit_stream_through_uncached() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/big.lance");
        inner
            .put(&loc, PutPayload::from(vec![1u8; 200_000]))
            .await
            .unwrap();

        // Tiny per-entry limit: a whole-object read of a 200 KB fragment exceeds it and must be
        // streamed straight through, never buffered to a cache file.
        let m = Arc::new(ObjectCacheMetrics::default());
        let c = CachingObjectStore::new(
            inner,
            tmp.path().to_path_buf(),
            64 * 1024 * 1024,
            1024,
            m.clone(),
        );

        let a = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let b = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(a.len(), 200_000, "full object still served");
        assert_eq!(a, b, "second read returns the same bytes");

        let (inner_gets, hits, misses, _, _) = m.snapshot();
        assert_eq!(hits, 0, "over-limit reads are never served from the cache");
        assert_eq!(
            misses, 0,
            "over-limit reads are never admitted, so never counted as cached"
        );
        assert_eq!(inner_gets, 2, "both reads were forwarded to the backend");

        // No cache entry file was written (only `.sz`/`.tmp` would be auxiliary, and none should exist).
        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                !n.ends_with(".sz") && !n.ends_with(".tmp")
            })
            .collect();
        assert!(
            entries.is_empty(),
            "no cache file should be written for an over-limit read"
        );
    }

    #[tokio::test]
    async fn reads_over_capacity_stream_through_even_under_entry_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/big.lance");
        inner
            .put(&loc, PutPayload::from(vec![2u8; 200_000]))
            .await
            .unwrap();

        // The entry limit (256 MiB) is far above the 64 KiB cap, so the effective limit is the cap:
        // a 200 KB read could never be retained and so is never buffered, written, or counted.
        let m = Arc::new(ObjectCacheMetrics::default());
        let c = CachingObjectStore::new(
            inner,
            tmp.path().to_path_buf(),
            64 * 1024,
            256 * 1024 * 1024,
            m.clone(),
        );

        let _ = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let (inner_gets, hits, misses, _, _) = m.snapshot();
        assert_eq!(
            (hits, misses),
            (0, 0),
            "over-capacity read is never cached nor counted"
        );
        assert_eq!(inner_gets, 1, "the read was forwarded to the backend");
        let files = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                !n.ends_with(".sz") && !n.ends_with(".tmp")
            })
            .count();
        assert_eq!(files, 0, "nothing written for an over-capacity read");
    }

    #[tokio::test]
    async fn payload_without_sidecar_is_not_served() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![7u8; 4096]))
            .await
            .unwrap();
        let (c, m) = cache(tmp.path(), inner);

        // Plant a payload file for the whole-object key but no `.sz` sidecar — i.e. an orphan from a
        // crash between rename and sidecar write. It must NOT be served as a hit.
        let (_key, path) = c.key_path(&loc, None);
        std::fs::write(&path, vec![0u8; 4096]).unwrap();

        let got = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            &got[..],
            &vec![7u8; 4096][..],
            "must serve the real bytes, not the sidecar-less orphan"
        );
        let (_, hits, misses, _, _) = m.snapshot();
        assert_eq!(
            hits, 0,
            "a payload without its sidecar must not count as a hit"
        );
        assert_eq!(
            misses, 1,
            "it re-fetched from the backend and cached properly"
        );
        // After re-fetch the sidecar now exists, so a repeat read is a real hit.
        let _ = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            m.snapshot().1,
            1,
            "repeat read is now a hit (sidecar present)"
        );
    }

    #[tokio::test]
    async fn startup_scan_removes_stale_tmp_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // A large leftover .tmp from a crashed write, plus a legitimate payload file.
        std::fs::write(dir.join("deadbeef.tmp"), vec![0u8; 500_000]).unwrap();
        std::fs::write(dir.join("cafef00d"), vec![1u8; 1024]).unwrap();

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let m = Arc::new(ObjectCacheMetrics::default());
        let c = CachingObjectStore::new(inner, dir.to_path_buf(), 64 * 1024 * 1024, u64::MAX, m);

        assert!(
            !dir.join("deadbeef.tmp").exists(),
            "stale .tmp must be deleted on startup, not leaked uncounted"
        );
        let used = c.lru.lock().unwrap().used;
        assert_eq!(
            used, 1024,
            "only the real payload counts toward the cap, not the .tmp leftover"
        );
    }

    #[tokio::test]
    async fn head_requests_are_never_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![9u8; 4096]))
            .await
            .unwrap();
        let (c, m) = cache(tmp.path(), inner);

        // HEAD must pass through and must NOT populate the whole-object cache key.
        let head = GetOptions {
            head: true,
            ..Default::default()
        };
        let _ = c.get_opts(&loc, head).await.unwrap();
        // a real whole-object GET must then fetch the genuine bytes (a miss), not a poisoned hit.
        let full = c
            .get_opts(&loc, GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            full.len(),
            4096,
            "real object served, not a HEAD-poisoned entry"
        );
        let (_, hits, misses, _, _) = m.snapshot();
        assert_eq!(
            (hits, misses),
            (0, 1),
            "HEAD passthrough left no cache entry; GET is a clean miss"
        );
    }

    #[tokio::test]
    async fn mutable_metadata_is_not_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest = OPath::from("ns/_versions/1.manifest");
        inner
            .put(&manifest, PutPayload::from(vec![1u8; 256]))
            .await
            .unwrap();
        let (c, m) = cache(tmp.path(), inner);

        let _ = c.get_opts(&manifest, GetOptions::default()).await.unwrap();
        let _ = c.get_opts(&manifest, GetOptions::default()).await.unwrap();
        let (inner_gets, hits, _, _, _) = m.snapshot();
        assert_eq!(
            hits, 0,
            "manifests must never be cached (mutable/version-numbered)"
        );
        assert_eq!(
            inner_gets, 2,
            "both manifest reads passed through to the inner store"
        );
    }

    #[tokio::test]
    async fn startup_scan_counts_existing_files_toward_capacity() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![5u8; 1024 * 1024]))
            .await
            .unwrap();

        // First store with a tiny cap caches one entry.
        {
            let m = Arc::new(ObjectCacheMetrics::default());
            let c = CachingObjectStore::new(
                inner.clone(),
                tmp.path().to_path_buf(),
                256 * 1024,
                u64::MAX,
                m,
            );
            let opts = GetOptions {
                range: Some(GetRange::Bounded(0..200_000)),
                ..Default::default()
            };
            let _ = c.get_opts(&loc, opts).await.unwrap().bytes().await.unwrap();
        }
        // A fresh store over the same dir must account for the on-disk file (used > 0), so a tiny
        // cap can't grow unbounded across restart.
        let m = Arc::new(ObjectCacheMetrics::default());
        let c = CachingObjectStore::new(inner, tmp.path().to_path_buf(), 256 * 1024, u64::MAX, m);
        let used = c.lru.lock().unwrap().used;
        assert!(
            used > 0,
            "startup scan must admit pre-existing files toward capacity, got {used}"
        );
    }

    #[tokio::test]
    async fn startup_scan_evicts_over_cap_oldest_first() {
        use filetime::{set_file_mtime, FileTime};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // Three 100 KiB cache files; mtimes ascending so k1 is oldest. Total 300 KiB > 256 KiB cap.
        let mk = |name: &str, secs: i64| {
            let p = dir.join(name);
            std::fs::write(&p, vec![0u8; 100 * 1024]).unwrap();
            set_file_mtime(&p, FileTime::from_unix_time(secs, 0)).unwrap();
            p
        };
        let p1 = mk("k1", 1000);
        let p2 = mk("k2", 2000);
        let p3 = mk("k3", 3000);

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let m = Arc::new(ObjectCacheMetrics::default());
        let c = CachingObjectStore::new(inner, dir.to_path_buf(), 256 * 1024, u64::MAX, m);

        let used = c.lru.lock().unwrap().used;
        assert!(
            used <= 256 * 1024,
            "used {used} must be within the byte cap after scan"
        );
        assert!(used > 0, "some entries retained");
        assert!(
            !p1.exists(),
            "oldest over-cap file must be evicted on startup"
        );
        assert!(p2.exists() && p3.exists(), "newer files retained");
    }

    #[tokio::test]
    async fn versioned_reads_are_not_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = OPath::from("ns/data/abc.lance");
        inner
            .put(&loc, PutPayload::from(vec![3u8; 4096]))
            .await
            .unwrap();
        let (c, m) = cache(tmp.path(), inner);

        // A versioned read resolves to different bytes than a plain (path, range) read, so it must
        // pass through uncached. (InMemory may or may not honour `version`; we only assert routing.)
        let opts = GetOptions {
            version: Some("v1".into()),
            ..Default::default()
        };
        let _ = c.get_opts(&loc, opts.clone()).await;
        let _ = c.get_opts(&loc, opts).await;
        let (inner_gets, hits, _, _, _) = m.snapshot();
        assert_eq!(hits, 0, "versioned reads must never be cached");
        assert_eq!(
            inner_gets, 2,
            "both versioned reads passed through to the inner store"
        );
    }
}
