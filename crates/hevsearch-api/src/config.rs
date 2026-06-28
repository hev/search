//! Env-var driven application config.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use hevsearch_core::{Scheme, StorageRoot};

use crate::auth::Secret;
use crate::rate_limit::RateLimitSettings;

/// Runtime configuration for the axum binary.
///
/// Auth-bearing fields (`api_key`, `admin_api_key`, `metrics_token`)
/// hold the redacting [`Secret`] newtype rather than raw strings so
/// `tracing::info!(?config)` cannot leak the configured API keys.
/// `storage_options` carries `object_store` parameters that may
/// include S3 credentials such as `aws_secret_access_key`; the
/// custom `Debug` impl below redacts the values for any key
/// recognised as credential-bearing instead of relying on the
/// derive.
#[derive(Clone)]
pub struct AppConfig {
    /// Address to bind the HTTP listener to.
    pub bind: SocketAddr,
    /// Object-storage root that every namespace lives under.
    /// Resolved from `HEVSEARCH_STORAGE_URI` (preferred) or the
    /// legacy `HEVSEARCH_S3_BUCKET` (fallback).
    pub storage_root: StorageRoot,
    /// RAM tier capacity for the foyer cache, in bytes.
    pub cache_memory_bytes: usize,
    /// Directory for the foyer NVMe-tier block file.
    pub cache_nvme_path: PathBuf,
    /// NVMe tier capacity, in bytes.
    pub cache_nvme_bytes: usize,
    /// Maximum request body size in bytes. Applied as a single
    /// router-level `DefaultBodyLimit` layer so every JSON endpoint
    /// inherits the same ceiling. Defaults to 16 MB; operators
    /// running larger multivector batches raise it via
    /// `HEVSEARCH_MAX_BODY_BYTES`.
    pub max_body_bytes: usize,
    /// `object_store`-style options passed straight through to
    /// `NamespaceManager` and, transitively, to lancedb. The exact
    /// keys depend on the resolved scheme: `aws_*` for S3-family
    /// backends, `google_*` for native GCS.
    pub storage_options: HashMap<String, String>,
    /// `HEVSEARCH_API_KEY` — required for the read/write tier when
    /// auth is enabled. `None` ⇒ disabled.
    pub api_key: Option<Secret>,
    /// `HEVSEARCH_ADMIN_API_KEY` — required for destructive ops
    /// when set. `None` ⇒ single-key fallback (write key
    /// authorises admin too).
    pub admin_api_key: Option<Secret>,
    /// `HEVSEARCH_METRICS_TOKEN` — gates `/metrics`. `None` ⇒
    /// `/metrics` is public (current pre-0.5.0 behaviour).
    pub metrics_token: Option<Secret>,
    /// Rate-limiter knobs. `None` everywhere ⇒ both limiters off.
    pub rate_limit: RateLimitSettings,
    /// Object cache (issue #51): when `true`, Lance object-store reads (index
    /// and data byte ranges) are served from a local-NVMe cache, cutting S3
    /// round-trips on warm/repeat/novel queries. `HEVSEARCH_OBJECT_CACHE_ENABLED`
    /// (default off).
    pub object_cache_enabled: bool,
    /// Directory for the object cache. `HEVSEARCH_OBJECT_CACHE_DIR`.
    pub object_cache_dir: PathBuf,
    /// Object cache on-disk capacity in bytes (LRU eviction).
    /// `HEVSEARCH_OBJECT_CACHE_BYTES` (default 10 GiB).
    pub object_cache_bytes: u64,
    /// Largest single read the object cache will buffer + cache; larger reads
    /// stream straight through uncached, bounding the RAM one miss can use.
    /// `HEVSEARCH_OBJECT_CACHE_MAX_ENTRY_BYTES` (default 256 MiB).
    pub object_cache_max_entry_bytes: u64,
    /// Cap on a single `/import` Arrow body, spooled to disk. The
    /// endpoint streams its body past the `DefaultBodyLimit`, so this is
    /// the replacement guard against filling the spool disk; exceeding it
    /// returns `413`. `HEVSEARCH_IMPORT_MAX_BYTES` (default 8 GiB; `0` ⇒
    /// unlimited).
    pub import_max_bytes: u64,
    /// Directory the `/import` handler spools the request body to before
    /// reading it as an Arrow stream. `HEVSEARCH_IMPORT_TMP_DIR` (default
    /// the system temp dir).
    pub import_tmp_dir: PathBuf,
}

/// True when the given storage-options key holds credential-bearing
/// material that must be redacted from `Debug` output. Matches by
/// substring (case-insensitive) so future credential keys
/// (`*_secret_access_key`, `*_session_token`, `*_password`,
/// vendor-specific names) are caught by the same check without
/// needing a code update.
fn is_sensitive_storage_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    // `service_account_key` covers GCS inline service-account JSON
    // (`google_service_account_key` / `service_account_key`) without
    // also redacting the harmless `service_account_path` form. Path
    // values are file locations, not the credentials themselves —
    // file contents are the secret, not the path.
    [
        "secret",
        "password",
        "token",
        "access_key",
        "credential",
        "service_account_key",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct RedactedOptions<'a>(&'a HashMap<String, String>);
        impl std::fmt::Debug for RedactedOptions<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut m = f.debug_map();
                for (k, v) in self.0 {
                    if is_sensitive_storage_key(k) {
                        m.entry(k, &"***");
                    } else {
                        m.entry(k, v);
                    }
                }
                m.finish()
            }
        }

        f.debug_struct("AppConfig")
            .field("bind", &self.bind)
            .field("storage_root", &self.storage_root)
            .field("cache_memory_bytes", &self.cache_memory_bytes)
            .field("cache_nvme_path", &self.cache_nvme_path)
            .field("cache_nvme_bytes", &self.cache_nvme_bytes)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("storage_options", &RedactedOptions(&self.storage_options))
            .field("api_key", &self.api_key)
            .field("admin_api_key", &self.admin_api_key)
            .field("metrics_token", &self.metrics_token)
            .field("rate_limit", &self.rate_limit)
            .finish()
    }
}

impl AppConfig {
    /// Load config from the environment. Either `HEVSEARCH_STORAGE_URI`
    /// (preferred) or the legacy `HEVSEARCH_S3_BUCKET` is required;
    /// everything else has a sensible default.
    pub fn from_env() -> anyhow::Result<Self> {
        let bind: SocketAddr = env_or("HEVSEARCH_BIND", "0.0.0.0:3000")
            .parse()
            .context("HEVSEARCH_BIND")?;
        let storage_root = resolve_storage_root_from_env()?;
        let cache_memory_bytes: usize = env_or("HEVSEARCH_CACHE_MEMORY_BYTES", "67108864")
            .parse()
            .context("HEVSEARCH_CACHE_MEMORY_BYTES")?;
        let cache_nvme_path =
            PathBuf::from(env_or("HEVSEARCH_CACHE_NVME_PATH", "/tmp/hevsearch-cache"));
        let cache_nvme_bytes: usize = env_or("HEVSEARCH_CACHE_NVME_BYTES", "268435456")
            .parse()
            .context("HEVSEARCH_CACHE_NVME_BYTES")?;
        let max_body_bytes: usize = env_or("HEVSEARCH_MAX_BODY_BYTES", "16777216")
            .parse()
            .context("HEVSEARCH_MAX_BODY_BYTES")?;

        let object_cache_enabled = env_bool("HEVSEARCH_OBJECT_CACHE_ENABLED", false)?;
        let object_cache_dir = PathBuf::from(env_or(
            "HEVSEARCH_OBJECT_CACHE_DIR",
            "/tmp/hevsearch-object-cache",
        ));
        let object_cache_bytes: u64 = env_or("HEVSEARCH_OBJECT_CACHE_BYTES", "10737418240")
            .parse()
            .context("HEVSEARCH_OBJECT_CACHE_BYTES")?;
        // Default 256 MiB; see hevsearch_core::object_cache::DEFAULT_MAX_ENTRY_BYTES.
        let object_cache_max_entry_bytes: u64 =
            env_or("HEVSEARCH_OBJECT_CACHE_MAX_ENTRY_BYTES", "268435456")
                .parse()
                .context("HEVSEARCH_OBJECT_CACHE_MAX_ENTRY_BYTES")?;

        // Default 8 GiB; `0` disables the cap.
        let import_max_bytes: u64 = env_or("HEVSEARCH_IMPORT_MAX_BYTES", "8589934592")
            .parse()
            .context("HEVSEARCH_IMPORT_MAX_BYTES")?;
        let import_tmp_dir = match std::env::var("HEVSEARCH_IMPORT_TMP_DIR") {
            Ok(dir) => PathBuf::from(dir),
            Err(_) => std::env::temp_dir(),
        };

        let storage_options = build_storage_options_for(storage_root.scheme());

        let api_key = optional_secret("HEVSEARCH_API_KEY");
        let admin_api_key = optional_secret("HEVSEARCH_ADMIN_API_KEY");
        let metrics_token = optional_secret("HEVSEARCH_METRICS_TOKEN");

        let trust_proxy_headers = env_bool("HEVSEARCH_TRUST_PROXY_HEADERS", false)?;
        let per_principal_rps = optional_u64("HEVSEARCH_RATE_LIMIT_RPS")?;
        let preauth_ip_rps = optional_u64("HEVSEARCH_PREAUTH_IP_LIMIT_RPS")?;
        let burst_size = optional_u32("HEVSEARCH_RATE_LIMIT_BURST")?;

        let rate_limit = RateLimitSettings {
            per_principal_rps,
            burst_size,
            preauth_ip_rps,
            trust_proxy_headers,
        };

        Ok(Self {
            bind,
            storage_root,
            cache_memory_bytes,
            cache_nvme_path,
            cache_nvme_bytes,
            max_body_bytes,
            storage_options,
            api_key,
            admin_api_key,
            metrics_token,
            rate_limit,
            object_cache_enabled,
            object_cache_dir,
            object_cache_bytes,
            object_cache_max_entry_bytes,
            import_max_bytes,
            import_tmp_dir,
        })
    }
}

/// Resolve the storage root from environment variables. Thin wrapper
/// over [`resolve_storage_root`] that reads `HEVSEARCH_STORAGE_URI`
/// and `HEVSEARCH_S3_BUCKET` from the process environment and emits a
/// legacy-fallback `INFO` log when only the latter is set. In normal
/// binary startup this is called once per process, but the wrapper
/// itself does not enforce that — callers that invoke it repeatedly
/// will get repeat logs.
fn resolve_storage_root_from_env() -> anyhow::Result<StorageRoot> {
    let uri_var = std::env::var("HEVSEARCH_STORAGE_URI").ok();
    let bucket_var = std::env::var("HEVSEARCH_S3_BUCKET").ok();
    let outcome = resolve_storage_root(uri_var.as_deref(), bucket_var.as_deref())?;
    if outcome.fallback_logged {
        tracing::info!(
            preferred = "HEVSEARCH_STORAGE_URI",
            legacy = "HEVSEARCH_S3_BUCKET",
            "Using HEVSEARCH_S3_BUCKET as legacy fallback. \
             HEVSEARCH_STORAGE_URI is the preferred env var; both are supported indefinitely."
        );
    }
    Ok(outcome.root)
}

/// Outcome of [`resolve_storage_root`]: the parsed [`StorageRoot`]
/// plus a flag indicating whether the legacy `HEVSEARCH_S3_BUCKET`
/// fallback path was taken. Callers consume the flag to decide
/// whether to emit the preference-hint `INFO` log; the resolver
/// itself is log-free so it can be exercised without capturing
/// tracing output.
#[derive(Debug, Clone)]
pub struct ResolvedStorageRoot {
    /// The parsed storage root.
    pub root: StorageRoot,
    /// True when only `HEVSEARCH_S3_BUCKET` was set and the resolver
    /// fell back to the legacy single-bucket form. Callers translate
    /// this into an `INFO` log pointing at the preferred env var.
    /// Both env vars are supported indefinitely — this is a
    /// preference hint, not a deprecation warning — so the message
    /// is intentionally low-volume and not enforced as one-shot.
    pub fallback_logged: bool,
}

/// Resolve a [`StorageRoot`] from raw `HEVSEARCH_STORAGE_URI` and
/// `HEVSEARCH_S3_BUCKET` values. Pure: no environment access, no
/// logging. Returns a [`ResolvedStorageRoot`] whose `fallback_logged`
/// flag tells the caller whether to emit the preference-hint log.
/// Precedence rules:
///
/// - `uri` only → parse it.
/// - `bucket` only → treat as `s3://{bucket}`. `fallback_logged` is true.
/// - Both → parse each; if the resulting [`StorageRoot`] values
///   compare equal, use the URI version silently; if they differ,
///   fail with both raw values in the error.
/// - Neither → fail with a message naming both env vars.
///
/// The "compare equal" check uses parsed normalised structs, not raw
/// strings, so `HEVSEARCH_STORAGE_URI=s3://foo` and
/// `HEVSEARCH_S3_BUCKET=foo` are recognised as agreeing.
pub fn resolve_storage_root(
    uri: Option<&str>,
    bucket: Option<&str>,
) -> anyhow::Result<ResolvedStorageRoot> {
    let uri = uri.map(str::trim).filter(|s| !s.is_empty());
    let bucket = bucket.map(str::trim).filter(|s| !s.is_empty());

    match (uri, bucket) {
        (None, None) => Err(anyhow::anyhow!(
            "set HEVSEARCH_STORAGE_URI=s3://bucket or HEVSEARCH_STORAGE_URI=gs://bucket, \
             or the legacy HEVSEARCH_S3_BUCKET=bucket"
        )),
        (Some(uri), None) => {
            let root = StorageRoot::parse(uri).with_context(|| {
                format!("HEVSEARCH_STORAGE_URI ({uri:?}) failed to parse as a storage URI")
            })?;
            Ok(ResolvedStorageRoot {
                root,
                fallback_logged: false,
            })
        }
        (None, Some(bucket)) => {
            let root = StorageRoot::s3_bucket(bucket).with_context(|| {
                format!("HEVSEARCH_S3_BUCKET ({bucket:?}) is not a valid bucket name")
            })?;
            Ok(ResolvedStorageRoot {
                root,
                fallback_logged: true,
            })
        }
        (Some(uri), Some(bucket)) => {
            let from_uri = StorageRoot::parse(uri).with_context(|| {
                format!("HEVSEARCH_STORAGE_URI ({uri:?}) failed to parse as a storage URI")
            })?;
            let from_bucket = StorageRoot::s3_bucket(bucket).with_context(|| {
                format!("HEVSEARCH_S3_BUCKET ({bucket:?}) is not a valid bucket name")
            })?;
            if from_uri == from_bucket {
                Ok(ResolvedStorageRoot {
                    root: from_uri,
                    fallback_logged: false,
                })
            } else {
                Err(anyhow::anyhow!(
                    "HEVSEARCH_STORAGE_URI and HEVSEARCH_S3_BUCKET disagree. \
                     HEVSEARCH_STORAGE_URI={uri:?} parses as {from_uri:?}, but \
                     HEVSEARCH_S3_BUCKET={bucket:?} parses as {from_bucket:?}. \
                     Set only one, or set both to consistent values."
                ))
            }
        }
    }
}

/// Build the `object_store`-style options map for the resolved
/// storage scheme. S3-family backends read the `HEVSEARCH_S3_*`
/// block; native GCS routes through service-account JSON loaded by
/// `GoogleCloudStorageBuilder::from_env` — there is no
/// hevsearch-specific env-var translation needed because the
/// standard `GOOGLE_*` vars are already what the underlying client
/// reads. We surface a couple of explicit options when set so an
/// operator can override the `from_env` defaults without exporting
/// `GOOGLE_*` (e.g. a deployment that wants the SA path scoped to
/// hevsearch rather than process-wide).
fn build_storage_options_for(scheme: Scheme) -> HashMap<String, String> {
    match scheme {
        Scheme::S3 => build_s3_storage_options(),
        Scheme::Gcs => build_gcs_storage_options(),
        // Local filesystem mode needs no credentials or endpoint
        // options; the directory is carried in the storage root.
        Scheme::Local => HashMap::new(),
    }
}

fn build_s3_storage_options() -> HashMap<String, String> {
    let mut opts = HashMap::new();
    if let Ok(v) = std::env::var("HEVSEARCH_S3_ENDPOINT") {
        // Custom endpoint implies MinIO / a local S3 emulator: force
        // path-style addressing and allow plain HTTP. A deployment
        // against real AWS leaves HEVSEARCH_S3_ENDPOINT unset and
        // skips this whole block.
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
    // Region resolution honours HEVSEARCH_S3_REGION, then the standard
    // AWS_REGION / AWS_DEFAULT_REGION variables, then a us-east-1
    // fallback. Setting it unconditionally here is deliberate: the
    // value is fed to lancedb's connection builder and to the
    // delete-path object_store builder, and an explicit region must
    // win over the bare AmazonS3Builder::from_env() default in the
    // latter. See hevsearch_core::resolve_s3_region for the precedence.
    opts.insert(
        "aws_region".into(),
        hevsearch_core::resolve_s3_region(|k| std::env::var(k).ok()),
    );
    opts
}

fn build_gcs_storage_options() -> HashMap<String, String> {
    // The native `object_store::gcp` client and lance-io's GCS
    // backend both call `from_env()` internally and pick up the
    // standard `GOOGLE_APPLICATION_CREDENTIALS` /
    // `GOOGLE_SERVICE_ACCOUNT_PATH` / `GOOGLE_SERVICE_ACCOUNT_KEY`
    // variables without our help. We only need to forward the SA
    // path or key when an operator wants it scoped to hevsearch
    // rather than the process — `GOOGLE_APPLICATION_CREDENTIALS`
    // and `GOOGLE_SERVICE_ACCOUNT_PATH` map to the same
    // `service_account_path`; `GOOGLE_SERVICE_ACCOUNT_KEY` is the
    // inline-JSON form. Empty values are skipped so an accidental
    // `GOOGLE_APPLICATION_CREDENTIALS=` export does not shadow a
    // valid path provided through the other vars.
    let mut opts = HashMap::new();
    for (env_key, opt_key) in [
        (
            "GOOGLE_APPLICATION_CREDENTIALS",
            "google_application_credentials",
        ),
        ("GOOGLE_SERVICE_ACCOUNT_PATH", "google_service_account_path"),
        ("GOOGLE_SERVICE_ACCOUNT_KEY", "google_service_account_key"),
    ] {
        if let Ok(v) = std::env::var(env_key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                opts.insert(opt_key.into(), trimmed.into());
            }
        }
    }
    opts
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn optional_secret(key: &str) -> Option<Secret> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(Secret::new(v)),
        _ => None,
    }
}

fn optional_u64(key: &str) -> anyhow::Result<Option<u64>> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Ok(Some(v.parse().with_context(|| key.to_string())?)),
        _ => Ok(None),
    }
}

fn optional_u32(key: &str) -> anyhow::Result<Option<u32>> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Ok(Some(v.parse().with_context(|| key.to_string())?)),
        _ => Ok(None),
    }
}

fn env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(key) {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" | "" => Ok(false),
            other => Err(anyhow::anyhow!(
                "{key} must be one of: true, false, 1, 0, yes, no, on, off — got {other:?}"
            )),
        },
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sensitive_storage_keys_are_recognised() {
        assert!(is_sensitive_storage_key("aws_secret_access_key"));
        assert!(is_sensitive_storage_key("aws_access_key_id"));
        assert!(is_sensitive_storage_key("AWS_SECRET_ACCESS_KEY"));
        assert!(is_sensitive_storage_key("aws_session_token"));
        assert!(is_sensitive_storage_key("gcs_credentials"));
        assert!(is_sensitive_storage_key("user_password"));
        // Inline GCS service-account JSON must be caught; the path
        // form is a file location, not the credential itself, and
        // stays visible so an operator can see at a glance which SA
        // file the deployment is pointing at.
        assert!(is_sensitive_storage_key("google_service_account_key"));
        assert!(is_sensitive_storage_key("service_account_key"));
        assert!(is_sensitive_storage_key("GOOGLE_SERVICE_ACCOUNT_KEY"));
        assert!(is_sensitive_storage_key("google_application_credentials"));
        assert!(!is_sensitive_storage_key("google_service_account_path"));
        assert!(!is_sensitive_storage_key("aws_endpoint"));
        assert!(!is_sensitive_storage_key("aws_region"));
        assert!(!is_sensitive_storage_key("allow_http"));
        assert!(!is_sensitive_storage_key(
            "aws_virtual_hosted_style_request"
        ));
    }

    #[test]
    fn debug_redacts_storage_credentials() {
        let mut opts = HashMap::new();
        opts.insert(
            "aws_secret_access_key".into(),
            "UNIQUE_S3_SECRET_DO_NOT_LEAK".into(),
        );
        opts.insert(
            "aws_access_key_id".into(),
            "UNIQUE_S3_ACCESS_DO_NOT_LEAK".into(),
        );
        opts.insert("aws_endpoint".into(), "http://minio:9000".into());
        opts.insert("aws_region".into(), "eu-west-1".into());

        let cfg = AppConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            storage_root: StorageRoot::s3_bucket("test").unwrap(),
            cache_memory_bytes: 0,
            cache_nvme_path: std::env::temp_dir(),
            cache_nvme_bytes: 0,
            max_body_bytes: 16 * 1024 * 1024,
            storage_options: opts,
            api_key: None,
            admin_api_key: None,
            metrics_token: None,
            rate_limit: RateLimitSettings::default(),
            object_cache_enabled: false,
            object_cache_dir: std::env::temp_dir(),
            object_cache_bytes: 0,
            object_cache_max_entry_bytes: 0,
            import_max_bytes: 0,
            import_tmp_dir: std::env::temp_dir(),
        };

        let dbg = format!("{:?}", cfg);
        assert!(
            !dbg.contains("UNIQUE_S3_SECRET"),
            "S3 secret leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("UNIQUE_S3_ACCESS"),
            "S3 access key leaked in Debug: {dbg}"
        );
        // Non-sensitive options must still be visible — they are
        // useful for diagnosing config (which endpoint? which region?).
        assert!(
            dbg.contains("http://minio:9000"),
            "non-sensitive endpoint should not be redacted: {dbg}"
        );
        assert!(
            dbg.contains("eu-west-1"),
            "non-sensitive region should not be redacted: {dbg}"
        );
    }

    #[test]
    fn debug_redacts_inline_gcs_service_account_json() {
        // The whole *point* of `google_service_account_key` is to
        // carry a service-account JSON blob inline. An operator who
        // logs `tracing::info!(?cfg)` must not have that JSON spilled
        // into the log; the path form (a file location) stays visible
        // because it is useful for diagnosing which deployment is
        // pointing where.
        let mut opts = HashMap::new();
        opts.insert(
            "google_service_account_key".into(),
            "{\"type\":\"service_account\",\"private_key\":\"UNIQUE_GCS_PRIVATE_KEY_DO_NOT_LEAK\"}"
                .into(),
        );
        opts.insert(
            "google_service_account_path".into(),
            "/etc/hevsearch/gcp-sa.json".into(),
        );
        opts.insert(
            "google_application_credentials".into(),
            "/etc/hevsearch/adc.json".into(),
        );

        let cfg = AppConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            storage_root: StorageRoot::parse("gs://hevsearch-gcs-test").unwrap(),
            cache_memory_bytes: 0,
            cache_nvme_path: std::env::temp_dir(),
            cache_nvme_bytes: 0,
            max_body_bytes: 16 * 1024 * 1024,
            storage_options: opts,
            api_key: None,
            admin_api_key: None,
            metrics_token: None,
            rate_limit: RateLimitSettings::default(),
            object_cache_enabled: false,
            object_cache_dir: std::env::temp_dir(),
            object_cache_bytes: 0,
            object_cache_max_entry_bytes: 0,
            import_max_bytes: 0,
            import_tmp_dir: std::env::temp_dir(),
        };

        let dbg = format!("{:?}", cfg);
        assert!(
            !dbg.contains("UNIQUE_GCS_PRIVATE_KEY"),
            "inline GCS service-account JSON leaked in Debug: {dbg}"
        );
        // Path entries are sensitive-credential-adjacent; the
        // application-credentials path is caught by `credential`,
        // and that is fine. The plain service-account *path* form is
        // a file location only, so it remains visible.
        assert!(
            dbg.contains("/etc/hevsearch/gcp-sa.json"),
            "service-account path should not be redacted: {dbg}"
        );
    }
}
