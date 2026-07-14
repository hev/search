# RFC 0003: Per-row delete (by id, then by filter)

Tracking issue: _TBD_

> **Status:** implemented. **Additive engine capability.** The engine now exposes
> `POST /ns/{ns}/delete` for delete-by-id and delete-by-filter, while
> `DELETE /ns/{ns}` remains the whole-namespace destructive operation. Layer's
> search backend maps portable delete-by-id to this route. This is storage/index
> behavior, squarely on the **engine** side of the engine/edge split
> (`CLAUDE.md`), and lands permanently in this hard fork.

## Summary

Add two engine operations:

1. **Delete by id** — remove a set of rows named by `id`.
2. **Delete by filter** — remove every row matching a DataFusion SQL predicate,
   the *same predicate dialect* the `/query` and `/facet` `filter` field and
   `/list` already accept.

Both compile to a single LanceDB `Table::delete(predicate)` call, so the second
is the first generalized: delete-by-id is `delete("id IN (…)")`. The keystone
plumbing — a SQL predicate threaded to LanceDB — already exists for query
prefiltering (`manager.rs:1391`, `:1411`, `only_if(f)`), so this is mostly wiring
a delete verb onto a predicate the engine already knows how to evaluate.

## Background: what delete does today

The only delete is namespace-scoped and physical:

- **Handler** — `crates/hevsearch-api/src/handlers.rs:225` (`DELETE /ns/{ns}`).
- **Service** — `crates/hevsearch-core/src/service.rs:187`.
- **Manager** — `crates/hevsearch-core/src/manager.rs:1070`: it enumerates and
  removes every object under the namespace's object-storage prefix
  (`meta.location`, `:1079`) and evicts the namespace's cached query results. It
  is the `DELETE` documented in `docs/api.html` as "Removes every object under the
  namespace's prefix … irreversible."

There is no row-level delete anywhere — not in the router (`crates/hevsearch-api/src/lib.rs`
has only `delete(handlers::delete)` on `/ns/{namespace}`) and not in the manager.
A row "goes away" only by being overwritten: upsert is latest-write-wins by `id`
(`manager.rs:865`, `merge_insert(&["id"])`), so you can *replace* a row but not
*remove* it.

## The constraint is already lifted: LanceDB deletes by predicate

LanceDB's `Table` exposes `delete(predicate: &str)`, evaluated as a DataFusion SQL
predicate over the table — the **same** dialect the engine already threads into
queries as `only_if`:

- Query prefilter — `manager.rs:1391` (`vq = vq.only_if(f.clone())`) and `:1411`
  (FTS `q.only_if(f.clone())`). The `filter` request field is documented as "a
  DataFusion SQL predicate … the same predicate dialect as `/list`."

So the predicate machinery the demo family depends on for filtered query is
exactly what delete needs. There is no new dialect, no new translator: a delete
predicate is a query predicate pointed at removal instead of ranking.

## Design

### Endpoint shape

A single route covers both cases, because by-id is a predicate special case:

```
POST /ns/{namespace}/delete
{ "ids": [12, 48, 1007] }              # delete by id
{ "filter": "section = 'recalled'" }   # delete by filter (DataFusion SQL)
```

- Exactly one of `ids` / `filter` is required; both set → `400`.
- `ids` compiles to `id IN (…)`; `filter` is passed through verbatim, validated
  the same way the query `filter` is (a malformed predicate is a `400`, mirroring
  `/query`).
- Admin vs read/write scope: delete is destructive, so it sits with the admin
  routes in the engine's pre-auth tiering today — but note auth itself is being
  removed (engine RFC 0002); post-0002 the route is open at the process boundary
  and gated by the `NetworkPolicy` like every other verb. The scope question is
  therefore moot once 0002 lands; until then, group it with the admin tier.

### Execution

1. Resolve the namespace handle (404 if the table does not exist, like every data
   route).
2. Build the predicate (`id IN (…)` or the verbatim `filter`).
3. `table.delete(&predicate).await` — one Lance commit. Lance delete is a
   metadata/deletion-vector operation, not a rewrite, so it is cheap and advances
   the table version like any other commit.
4. **Invalidate caches.** A delete is a committed change, so it must drop the
   namespace's exact + semantic query caches exactly as upsert/compact/index do
   (`README.md` cache-invalidation contract: "Any committed change drops both
   layers"). The version bump the delete commit produces is what the cache keys on,
   so this is the existing invalidation path, not new logic.

### Response

Return rows removed so callers can distinguish "deleted N" from "matched
nothing":

```json
{ "deleted": 3 }
```

(Lance reports affected rows from the delete commit; if obtaining an exact count
is costly, a `200` with a best-effort count is acceptable — define in the
implementation PR.)

### Sync vs async

Delete is a single commit and far cheaper than `/import` or an index build, so it
is **synchronous** (`200`), not a `202` + `/operations/{id}` job. A pathological
whole-table filter is still one commit; if a future need arises for a very large
ranged purge, an async variant can follow, but v1 is sync.

## Interaction with known engine quirks

- **Duplicate ids from the append-only era.** `/upsert` is now idempotent, but
  namespaces that accumulated duplicate ids under earlier append-only versions can
  still hold several rows for one id (`docs/api.html` upgrade note; issue #68).
  `delete {ids:[…]}` as `id IN (…)` removes **all** rows for those ids, which is
  the desired semantics and incidentally a manual remedy for the dedupe backlog.
- **`_ingested_at` / cursor.** Delete removes rows; it does not touch the
  `_ingested_at` ordering of survivors, so `/list` pagination is unaffected beyond
  the rows vanishing.
- **Indexes.** Deletion vectors are honored by scans and by the IVF_PQ / FTS /
  scalar indexes; a later `/compact` materializes the deletions and reclaims space
  (compaction already rewrites fragments — `manager.rs` compact path). No index
  rebuild is required for correctness.

## Edge mapping (how Layer uses this)

For the edge twin (RFC 0086): the portable `VectorStoreClient::delete_ids`
(Turbopuffer-shaped delete-by-id) maps to `POST /ns/{ns}/delete {ids}`; the
Turbopuffer filter-write delete maps to `{filter}` via the gateway's existing
`turbolisp → SQL` translator (the one already targeting the query `filter` field).
`delete_all` continues to map to the namespace `DELETE`. Until this RFC lands,
both per-row deletes are `422 UnsupportedByStore` on a `search` namespace, which is
the current honest matrix state (`../layer/site/src/content/docs/kubernetes/store-support.mdx`).

## Phasing

1. **Delete by id.** The narrower, higher-traffic case: `{ids}` → `id IN (…)` →
   `table.delete` → cache invalidation → count. Ship with the integration test
   below.
2. **Delete by filter.** Generalize the same handler to accept `{filter}`, reusing
   the query-path predicate validation. No new engine machinery — it is the same
   `table.delete` with a caller-supplied predicate.

(The two are one PR if convenient; the split only reflects that by-id is the
must-have and by-filter rides on top.)

## Testing

- **Integration** (`crates/hevsearch-api/tests/`): upsert N rows, delete a subset
  by id, assert `/query` and `/list` no longer surface them and `row_count`
  (`GET /ns/{ns}`) drops by the right amount; delete by `filter` and assert the
  same; malformed `filter` → `400`; both `ids` and `filter` → `400`; delete from a
  nonexistent namespace → `404`.
- **Cache**: assert a delete invalidates the exact result cache (a repeat of a
  previously-cached query does not return a deleted row) — extend the
  `service_cache_aside` tests.
- **Dedupe**: a namespace seeded with duplicate ids (append-only era) has all
  copies removed by a single by-id delete.

## Alternatives considered

- **Tombstone-via-upsert.** Add a reserved `_deleted` column and filter it out of
  every read. Rejected: it bloats every query with a mandatory predicate, never
  reclaims storage without a separate sweep, and reimplements what LanceDB's
  deletion vectors already do natively. `table.delete` is strictly simpler.
- **Delete-by-id only, forever.** Tempting (it is the common case), but the
  predicate path is nearly free given query already threads `only_if`, and the
  demo family's openFDA / recall flows want filtered purge. Ship both.
- **Do it in the gateway (post-filter / re-upsert dance).** Rejected hard: it
  violates the engine/edge split (`CLAUDE.md` § "The one rule") — deletion is an
  engine/storage operation, and a gateway-side emulation cannot reclaim storage or
  honor deletion vectors. The fix lives in the engine.

## Fork delta

Pure **additive engine capability** on a hard fork — lands here, stays here, no
upstream PR (`AGENTS.md` § "This is a hard fork"). Record the new route and the
`table.delete` usage so a hand cherry-pick of an upstream change doesn't collide
with it. No subtractive edge removal here.

## References

- `crates/hevsearch-core/src/manager.rs:1070` — namespace delete (the only delete
  today); `:1079` — prefix enumeration; `:1391`/`:1411` — `only_if` query
  prefilter (the predicate plumbing this reuses); `:865` — `merge_insert(&["id"])`
  (latest-write-wins upsert, the only current "removal" by overwrite).
- `crates/hevsearch-core/src/service.rs:187`, `crates/hevsearch-api/src/handlers.rs:225`,
  `crates/hevsearch-api/src/lib.rs` (router) — the delete call chain to extend.
- `docs/api.html` — current `DELETE /ns/{ns}` semantics and the duplicate-id
  upgrade note (issue #68).
- engine RFC 0002 — auth removal; settles the delete-route scope question (open
  at the process boundary behind a `NetworkPolicy`).
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the edge twin;
  `delete_ids` / `delete_all` mapping and the `422 UnsupportedByStore` the engine
  returns until this lands.
- `CLAUDE.md` § "Engine (keep) vs edge (shed)", `AGENTS.md` § "The engine/edge test".
