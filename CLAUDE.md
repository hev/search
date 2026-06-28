# hev search — engine build context

This is **hev search**, the proprietary vector / FTS / hybrid search engine that
runs **behind hev layer** (origin `github.com/hev/search`). It began as a fork of
[firnflow](https://github.com/gordonmurray/firnflow) by Gordon Murray, but it is
now a **hard fork** — developed as ours, no longer feeding upstream. The
`README.md` carries the public product framing; *this* file is the internal frame
for the engine's purpose. The Layer-side strategy lives in
`../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` (RFC 0086) — read it
before reasoning about direction.

## The one frame: search is the engine, Layer is the edge

hev layer (the gateway in `../layer`) fronts this engine. The division of labor
is the most important thing to internalize, because it decides what belongs here
and what does not:

- **search owns the engine** — vector / FTS / hybrid search, LanceDB storage on
  object storage, the IVF_PQ / BM25 / scalar index lifecycle, and the foyer
  (L1 RAM / L2 NVMe) + object + result + semantic caches. This is the moat. Keep
  it and deepen it.
- **Layer owns the edge** — auth, per-tenant authorization, rate limiting, the
  inbound (Turbopuffer-shaped) wire, query history, embedding, and cost. The
  engine runs behind a `NetworkPolicy` reachable only by the Layer gateway (data
  path) and operator (admin path).

The rule that falls out: **don't build into the engine what Layer owns, and don't
make Layer reimplement what the engine owns.** A new auth / rate-limit / tenancy
feature here is a smell; so is gateway-side filtering / fusion / facet math in
Layer.

## Engine (keep) vs edge (shed)

Because Layer is the edge, the fork **sheds the edge features** stock firnflow
carries for being internet-facing:

- **Shed:** auth (`HEVSEARCH_API_KEY` / `HEVSEARCH_ADMIN_API_KEY` /
  `HEVSEARCH_METRICS_TOKEN`), rate limiting (`HEVSEARCH_RATE_LIMIT_RPS` /
  `_BURST`), `HEVSEARCH_TRUST_PROXY_HEADERS`, the preauth IP limiter. Layer is the
  auth boundary; the engine is a trusted internal service. **Auth is the flagship
  removal** — tracked as engine RFC 0002 (`docs/rfcs/0002-remove-auth.md`).
- **Keep:** everything in the engine list above — including the internal caches,
  even though Layer also runs a document cache. They are not duplicates:
  the engine's caches live *at the data* (query results, S3 byte-ranges); Layer's
  document cache adds **point-fetch-by-id**, which the engine lacks. Complementary,
  not redundant.

## This is a hard fork

The fork has stopped tracking upstream. That changes how every delta is handled:

- **It's ours now.** Additive engine capability (filter, arbitrary metadata
  columns, facets) and subtractive edge removals (auth, rate limiting) are **both
  local and permanent**. There is no "additive work goes upstream" path anymore —
  **we send nothing back to `gordonmurray/firnflow`.**
- **We no longer rebase on pinned upstream releases.** The tracked-fork discipline
  is dropped. If a specific upstream fix is worth having, cherry-pick it by hand;
  don't reintroduce the whole upstream surface, and never let a pull silently
  re-add a deliberately-removed edge feature (auth, rate limiting).
- **The original copyright + license are retained** in `LICENSE` (the engine
  source stays public, Apache-2.0). The hard fork is a development posture, not a
  relicensing.

This is intentional divergence, not debt. The engine is a first-party product with
its **own** issues and RFCs (`search/docs/rfcs/`) — it no longer borrows Layer's
paper trail for engine work.

## What the engine is NOT (Layer owns it)

Don't add these here; they live in `../layer`:

- **Auth / tenancy / authorization** — Layer's scoped keys bind a caller to its
  namespace(s). The engine keeps only its physical namespace = object-storage-prefix
  isolation; the *authorization* is Layer's.
- **Query history / clickstream** — Layer logs queries to S3; the engine has
  caches, not a query log. (The engine stores no history of its own.)
- **Embedding** — the engine is bring-your-own-vector. Layer's pipeline embeds and
  POSTs vectors; the engine stores and searches them.
- **The inbound wire** — clients speak Layer's Turbopuffer-shaped API; the engine's
  REST surface is internal.

## Pointers

- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the Layer-side
  strategy (fork-as-engine, the capability matrix, the open-stack deploy).
- `README.md` — public product framing + build/test (containerized cargo,
  MinIO-gated tests, storage-backend matrix).
- `docs/rfcs/` — the engine's own RFCs (where engine capability gaps land).
- `AGENTS.md` — the engineering rules for changing the engine.
