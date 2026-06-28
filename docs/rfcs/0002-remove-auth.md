# RFC 0002: Remove auth, rate limiting, and proxy-header trust

Tracking issue: _TBD_

> **Status:** draft, proposal. **The flagship subtractive removal.** The engine
> runs *behind* Layer, reachable only by the Layer gateway (data path) and the
> operator (admin path) over a `NetworkPolicy`; Layer is the auth boundary and the
> engine's only client. Stock firnflow's internet-facing edge — API-key auth, the
> per-principal + preauth-IP rate limiters, and proxy-header trust — is **dead
> weight here and a boundary error to keep** (`CLAUDE.md` § "Engine (keep) vs edge
> (shed)"; `AGENTS.md` § "The engine/edge test"). This RFC deletes it: two whole
> modules (`auth.rs`, `rate_limit.rs`), their wiring, two test files, three
> dependencies, and the `HEVSEARCH_API_KEY` / `HEVSEARCH_ADMIN_API_KEY` /
> `HEVSEARCH_METRICS_TOKEN` / `HEVSEARCH_RATE_LIMIT_*` / `HEVSEARCH_TRUST_PROXY_HEADERS`
> config surface — ~900–1000 lines of Rust.
>
> The engine is a **hard fork** — nothing goes back to upstream regardless of
> shape (`CLAUDE.md` § "This is a hard fork"). This removal is local and
> permanent; record it as a deliberate divergence so a future hand cherry-pick
> from upstream never silently re-adds it.

## Summary

Auth, rate limiting, and proxy-header trust are **edge** concerns. The engine/edge
split (`CLAUDE.md`) puts them in Layer — scoped keys bind a caller to its
namespace(s), Layer does per-tenant authorization, Layer rate-limits. The engine
keeps only what is physical and engine-shaped: **namespace = object-storage-prefix
isolation**. Everything that validates a caller's *identity* or *budget* comes
out. After this RFC the engine is an unconditionally-open service at the process
boundary, secured by the `NetworkPolicy` that lets only Layer and the operator
reach it.

## Motivation: the engine/edge test, applied

Run the test from `AGENTS.md`: *is this engine or edge?*

- API-key validation, bearer parsing, principal/scope checks → **edge** (identity).
- Per-principal and preauth-IP rate limiting → **edge** (budget/abuse, a property
  of the internet-facing front Layer is).
- `trust_proxy_headers` / `X-Forwarded-For` peer-IP extraction → **edge** (only
  meaningful when you are the thing behind the proxy; here Layer is).

All three are edge, so all three leave. Keeping them would force two auth layers
in series (Layer's scoped keys *and* the engine's global bearer), buy nothing —
the engine already can't be reached except through Layer — and contradict the
fork's whole reason for existing. The engine is a **trusted internal service**.

What is **not** removed, because it is engine, not edge:

- **Physical namespace isolation** — a namespace is an object-storage prefix
  (`s3://bucket/ns/`). That is storage layout, not authorization; it stays.
- **`/health` and `/metrics`** stay as endpoints. They simply stop being
  protected: `/metrics` loses its token gate and is reachable by anything inside
  the `NetworkPolicy` (the Prometheus scraper). Network scope replaces the token.

## What gets removed

Grounded in the current code surface:

**Deleted outright**

| Path | What |
|---|---|
| `crates/hevsearch-api/src/auth.rs` | the entire auth module — `Secret`/`ct_eq`, `Principal`/`Scope`, `AuthConfig`, `require_write`/`require_admin`/`require_metrics_token`, bearer parsing, `peer_ip`/`forwarded_ip` (~658 lines) |
| `crates/hevsearch-api/src/rate_limit.rs` | the entire rate-limit module — governor limiter builders, key extractors, 429 response mapping (~292 lines) |
| `crates/hevsearch-api/tests/api_auth.rs` | auth integration tests |
| `crates/hevsearch-api/tests/api_rate_limit.rs` | rate-limit integration tests |

**Modified**

| Path | Change |
|---|---|
| `crates/hevsearch-api/src/lib.rs` | drop the `auth`/`rate_limit` module exports and the router split — no `require_*` `route_layer`s, no principal/IP limiter stacking, no protected-vs-public sub-routers. One flat router; `/health` and `/metrics` plain. |
| `crates/hevsearch-api/src/config.rs` | remove `api_key`/`admin_api_key`/`metrics_token`/`rate_limit` fields, the `HEVSEARCH_*` reads (`:190`–`:197`), the `optional_secret`/`optional_u64`/`optional_u32`/`env_bool` helpers if unused elsewhere, and the secret-redaction Debug paths that exist only for these. |
| `crates/hevsearch-api/src/state.rs` | remove `build_auth_config()`, the `auth`/`rate_limit` fields on `AppState`, and `auth_state()`. |
| `crates/hevsearch-api/src/error.rs` | remove the `Unauthorized` / `Forbidden` / `RateLimited` variants and their 401/403/429 responses (incl. `WWW-Authenticate` / `Retry-After`). |
| `crates/hevsearch-core/src/metrics.rs` | remove the `hevsearch_auth_rejections_total` counter, its field, `record_auth_rejection()`, and the test accessor. |
| `crates/hevsearch-api/tests/common/mod.rs` | collapse the `test_state*_with_auth` variants and `dummy_config`/`secret` helpers to the single no-auth shape. |

**Dependencies dropped** (`Cargo.toml` + `crates/hevsearch-api/Cargo.toml`)

- `tower_governor = "0.7"` — rate-limit middleware (auth/rate-limit only).
- `governor = "0.8"` — underlying limiter (named only to spell `NoOpMiddleware`).
- `subtle = "2"` — constant-time compare (only the auth secret path uses it).

Confirm none are pulled in elsewhere before removing the workspace entries.

**Docs / config**

- `README.md` — delete the auth/rate-limit env-var tables and the per-endpoint
  auth column.
- `CHANGELOG.md` — add the removal entry (below); the 0.5.0 "added auth" note
  stays as history.
- `docker-compose.yml` / `Dockerfile` — already set none of these vars; no change.

## Posture after removal

The engine becomes **unconditionally open at the process boundary**. This must be
stated loudly in the README and in the boot log: the only thing standing between
the network and the data is the `NetworkPolicy`. That is the intended design —
Layer is the auth boundary — but it is a footgun if someone runs the engine
exposed. A single startup log line ("running without authentication; expects to
sit behind a trusted gateway / NetworkPolicy") replaces the old open-mode warning.

No defense-in-depth bearer is re-added. Per `CLAUDE.md`, auth is *the* flagship
removal; a "thin optional key just in case" would re-introduce the exact edge
feature this RFC exists to shed, and the engine/edge test would flag it. If the
engine ever needs to be reachable by something that isn't Layer, that is a new
RFC, not a leftover.

## Hard fork — stays local, permanently

The engine is a hard fork that no longer feeds upstream (`CLAUDE.md` § "This is a
hard fork"): additive *and* subtractive deltas are both ours and permanent, and
**nothing is sent back to `gordonmurray/firnflow`**. The only upstream-adjacent
risk is a future hand cherry-pick pulling in a fix that drags auth back with it —
guard against that the same way `CLAUDE.md` mandates ("never let a pull silently
re-add a deliberately-removed edge feature"). **Record it** as a deliberate
divergence in the CHANGELOG so it reads as intentional and is never silently
undone:

> **Removed** — authentication, rate limiting, and proxy-header trust. The engine
> runs behind hev layer, which is the auth boundary; the engine is a trusted
> internal service on a `NetworkPolicy`. Deleted `auth`/`rate_limit` modules, the
> `HEVSEARCH_API_KEY`/`HEVSEARCH_ADMIN_API_KEY`/`HEVSEARCH_METRICS_TOKEN`/
> `HEVSEARCH_RATE_LIMIT_*`/`HEVSEARCH_TRUST_PROXY_HEADERS` config, and the
> `subtle`/`governor`/`tower_governor` dependencies.

## Migration / deployment impact

- The removed env vars become **inert**: a deployment that still sets
  `HEVSEARCH_API_KEY` etc. is harmless (the engine simply ignores unknown env),
  but the README should tell operators to stop setting them. No data migration,
  no API-shape change for legitimate (Layer / operator) callers.
- Clients that previously sent `Authorization: Bearer …` still work — the header
  is now ignored rather than validated. Layer does not depend on the engine
  authenticating it.
- Metrics scraping moves from token-gated to network-scoped. Ensure the scraper
  is inside the `NetworkPolicy`.

## Testing

- Delete `api_auth.rs` and `api_rate_limit.rs`; the remaining integration suite
  (`api_fts.rs`, the manager tests, etc.) must stay green through the router
  simplification.
- Simplify `tests/common/mod.rs` to one state constructor.
- Add a small assertion that the formerly-protected routes (read, admin) are
  reachable **without** any `Authorization` header — i.e. open mode is the only
  mode — so a future rebase or refactor that re-introduces a gate fails loudly.

## Open questions

- **`trust_proxy_headers` / peer-IP for logging.** Today peer-IP extraction
  exists only to feed the IP limiter. Layer owns request logging, so the default
  is to remove it entirely. Confirm nothing in the engine's own tracing wants a
  client IP before deleting `forwarded_ip`.
- **`/metrics` exposure.** Leave it open inside the `NetworkPolicy` (recommended),
  or bind it to a separate operator-only port? Network scope is the simpler answer
  and matches "engine is trusted internal."
- **Config helper fate.** Decide per-helper whether `optional_*`/`env_bool` have
  non-auth callers; remove only the ones that don't.

## References

- `crates/hevsearch-api/src/auth.rs`, `crates/hevsearch-api/src/rate_limit.rs` —
  the modules deleted.
- `crates/hevsearch-api/src/lib.rs` (router split), `config.rs:190`–`:197`
  (env reads), `state.rs:158`–`:206` (`build_auth_config`), `error.rs:23`–`:30`
  (auth error variants), `hevsearch-core/src/metrics.rs` (`auth_rejections`).
- `Cargo.toml:62`/`:66`/`:71` — `tower_governor`/`governor`/`subtle`.
- `CLAUDE.md` § "Engine (keep) vs edge (shed)" — auth is the canonical removal;
  `AGENTS.md` § "Change protocol" — subtractive removals stay local and get
  recorded.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` § "The engine is
  headless; Layer is the edge" — the edge view: Layer supplies `deriveFromStore` +
  scoped keys, closing the tenancy gap stock firnflow defers to "a gateway".
