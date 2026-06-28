# RFC 0007: Point-fetch by id and nearest-to-stored-id query

Tracking issue: _TBD_

> **Status:** draft, proposal. **Additive engine capability.** The engine can ANN
> on a **caller-supplied vector** (`query().nearest_to(v)`, `manager.rs:1488`) but
> cannot ANN on the vector of a **row it already stores** — there is no
> `nearest_to_id`. Layer's `recommend` / "more like this" surface
> (`shop`'s `query_namespace(nearest_to_id=[asin])`) therefore has to round-trip
> the seed vector out of the engine and back in, and there is no cheap point-read
> to get it (no `GET /ns/{ns}/doc/{id}`; fetch-by-id is an emulated `/list` scan).
> This sits on the **engine** side of the split — it needs the stored vector and
> the ANN index, both of which live here — so the fix is here. Hard fork: lands
> here, stays here, no upstream PR (`AGENTS.md` § "This is a hard fork"). The edge
> twin is `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md`.

## Summary

Add **nearest-to-stored-id** search: a `/query` seeded by an existing row's id
instead of a caller-supplied vector. The engine resolves the seed row's vector
internally, runs the normal ANN path with it, and excludes the seed from the
results — so a "more like this product / patient / moment" query is **one engine
call over data the engine already holds**, not a gateway round-trip that ships a
768-float vector each way.

A small, **optional** companion — a point-read route `GET /ns/{ns}/doc/{id}` — is
named for the gateway's document-cache miss-fill path, but is explicitly secondary
and boundary-fenced (point-fetch-by-id is Layer's job; see "Why this is the right
layer"). The load-bearing capability is `nearest_to_id`, which the document cache
**cannot** serve because it is a vector search, not a row lookup.

## Background: the engine can seed ANN by vector but not by id

`/query` (`crates/hevsearch-api/src/lib.rs:73`) takes a query **shape** and runs
LanceDB ANN:

| Site | Code | Role |
|---|---|---|
| Single-vector ANN | `manager.rs:1488` (`.nearest_to(v.clone())`) | seed the search with a **caller** vector |
| Multivector ANN | `manager.rs:1492-1504` (`.nearest_to(...).add_query_vector(...)`) | MaxSim late-interaction |
| Prefilter | `manager.rs:1511` (`.only_if(filter)`) | DataFusion predicate before ranking |
| k / projection | `manager.rs:1506` (`.limit(k)`), `:1514` (`Select::columns`) | result size + vector exclusion |

There is **no** way to say "rank by the vector of row `id`." The only id-keyed
read paths today are:

- **No point-read.** `lib.rs` exposes no `GET /ns/{ns}/doc/{id}`. Fetch-by-id is
  emulated by `/list` (`lib.rs:75`) with an `id` filter — an **ordered scan**
  (`manager.rs:1806`, cursor by `(_ingested_at, id)`), not an O(1) lookup, even
  though an auto BTree on `id` already exists (built on first write).
- **Filter-by-id exists internally.** `delete_ids` (`manager.rs:1182`) builds
  `id IN (…)` via `RowId::to_sql_literal` (`manager.rs:163`, which quotes string
  ids) and hands it to a predicate. That is exactly the primitive a seed-vector
  take needs — `only_if("id = <lit>")` projecting `vector`, `limit 1`.

The id type is already an enum — `RowId::U64 | RowId::String` (`result.rs:18`),
`RowIdType` fixed per namespace (`result.rs:200`, RFC 0005) — so a seed id is
typed against the namespace exactly like a delete id.

## Why this is the right layer (engine, not a gateway round-trip)

`CLAUDE.md` is explicit that **point-fetch-by-id is Layer's** (the document cache
"adds point-fetch-by-id, which the engine lacks. Complementary, not redundant").
This RFC does **not** dispute that for the row-lookup case. It draws the line at
the **vector** case:

- **`nearest_to_id` is a vector search, not a row lookup.** It needs the stored
  vector *and* the ANN index — both engine-resident. The document cache holds
  rendered documents for display, not the ANN structure; it cannot answer "what is
  near this row." So this capability has no edge analog to defer to.
- **The gateway emulation is two hops + a vector on the wire.** Without
  `nearest_to_id`, Layer must (1) read the seed row's vector out of the engine,
  then (2) POST it back as a `vector` query — two HTTP round-trips per recommend,
  shipping a full vector each way, and step (1) is itself the missing cheap read.
  Resolving the seed **inside** the engine, next to the data and the index, is the
  data-local placement; it is the same "don't make the gateway reimplement what the
  engine owns" smell (`CLAUDE.md`) the metadata-column and filter work already
  settled.

So **seed-by-id ANN** belongs in the engine. The **point-read** route is a
different question, handled as a fenced, optional secondary below.

## Design

### `nearest_to_id` on `/query` (the capability)

Add an optional `nearest_to_id` to the `/query` body, **mutually exclusive** with
the `vector` query shape (a `400` if both, like any over-specified query):

```
POST /ns/{ns}/query
{ "nearest_to_id": "<id>", "filter": "...", "top_k": 20, "include_vector": false }
```

Execution, entirely engine-side:

1. **Type-check the seed id** against the namespace `id_type` (`result.rs:200`),
   same guard as `delete_ids` (`manager.rs:1194`); wrong type → `400`.
2. **Take the seed vector.** Run a one-row read — `only_if(format!("id = {}",
   id.to_sql_literal()))` over the table, projecting only `vector`, `limit 1`
   (the auto BTree on `id` makes this a lookup, not a scan). Missing id → `404`.
3. **ANN with the seed vector.** Feed it into the existing single-vector path
   (`manager.rs:1488`) — same `nprobes`/`limit`/`only_if`/projection plumbing, so
   `filter`, `top_k`, `include_vector`, facets all compose unchanged.
4. **Exclude the seed.** Append `id != <lit>` to the prefilter (or drop it
   post-rank) so a row is never its own nearest neighbour. Document which (filter
   is cleaner and keeps `top_k` honest).

Multivector namespaces: the seed is the stored token bag; reuse the multivector
path (`manager.rs:1492`). Gate behind the same `include_vector` cost rules
(multivector seeds are large; the seed read projects `vector` only, never returns
it). If multivector seed-take proves heavy, v1 may restrict `nearest_to_id` to
single-vector and `422` multivector — call it in the PR.

### `GET /ns/{ns}/doc/{id}` (optional, secondary, boundary-fenced)

A point-read returning one row in full — `{id, vector?, text?, attributes,
ingested_at}` — keyed by the id BTree (O(1), not the `/list` scan). It is **not**
the headline and is **not** a replacement for Layer's document cache:

- Its only job is to make the cache **miss-fill** (and any `fetch_vector`) an
  indexed lookup instead of an ordered `/list` scan. The cache still fronts
  point-fetch; this is the authoritative read behind it.
- Because `CLAUDE.md` assigns point-fetch-by-id to the edge, this route is
  **deferrable**: if the `/list`+filter emulation is fast enough in practice
  (Layer's `shop` Wave verification will tell), it need not ship at all. Ship it
  only if a measured miss-fill cost justifies it.

`include_vector` defaults to `true` here (unlike `/query`), mirroring `/list`
which always carries the vector (`result.rs:131`) — the cache wants the whole row.

### What changes per site

1. **API body** (`crates/hevsearch-api/src/`): `/query` request gains
   `nearest_to_id: Option<RowId>`; validation rejects it alongside `vector`.
2. **Manager** (`crates/hevsearch-core/src/manager.rs`): a `seed_vector(ns, id)`
   helper (the take in step 2, reusing `to_sql_literal` + `only_if`); the query
   path branches to it before `.nearest_to`. No schema change — vectors and the id
   BTree already exist.
3. **Optional route** (`lib.rs` read tier): `GET /ns/{ns}/doc/{id}` → a
   manager `get_row(ns, id)` (same take, full projection).
4. **No admin/index/schema change.** This is read-path only; no migration.

## Edge mapping (how Layer uses this)

- Layer's `nearest_to_id` (the SDK field `shop` calls — `recommend`, "more like
  this") maps **directly** to the new `/query` `nearest_to_id`: one engine call,
  no seed vector on the wire, the seed excluded server-side. Until this lands,
  Layer emulates it (read seed vector, re-query) and the route is correct but
  two-hop.
- Plain point-fetch-by-id stays Layer's document cache. The optional
  `GET …/doc/{id}` only sharpens the cache's miss-fill; it does not move the
  boundary.
- String seed ids ride RFC 0005 (the seed id is typed like any id); the `id != `
  exclusion quotes string ids via `to_sql_literal`, same as delete (RFC 0003).

## Open questions (for the implementation PR)

- **Exclude via filter vs post-rank.** `id != <lit>` in `only_if` keeps `top_k`
  exact but adds a predicate term; post-rank drop is simpler but returns `k-1` on
  a self-hit. Prefer the filter; confirm DataFusion plans it cheaply alongside the
  user filter.
- **Multivector seed in v1.** Ship single-vector `nearest_to_id` first and `422`
  multivector, or do both? Decide on the seed-take cost.
- **Ship the point-read at all?** Gate on Layer's measured miss-fill cost
  (`shop`). If `/list`+filter is fine, defer `GET …/doc/{id}` indefinitely.
- **`404` vs empty.** Missing seed id → `404` (distinct from "found, no
  neighbours" → `200` empty). Confirm Layer maps `404` to a clean client error,
  not a `422`.

## Testing

- **Integration** (`crates/hevsearch-api/tests/`): seed an `id`, assert
  `nearest_to_id` returns its neighbours with the **seed absent**; a filter +
  `nearest_to_id` prefilters correctly; `top_k` honored; a missing id → `404`;
  `nearest_to_id` + `vector` together → `400`; string-id namespace (RFC 0005)
  seeds and excludes with quoted ids. Single- and (if in scope) multivector.
- **Point-read** (if shipped): `GET …/doc/{id}` returns the full row incl vector;
  missing → `404`; matches the row `/list` would yield.
- Existing `/query` and `/list` tests stay green (the new field is additive).

## Alternatives considered

- **Gateway emulation only (status quo).** Read the seed vector, re-POST it as a
  `vector` query. Correct, but two hops and a vector on the wire per recommend, and
  step one is the missing cheap read — so the gateway pays twice for something the
  engine answers once next to the data. Acceptable as the *interim*, not the design.
- **A bare point-read, no `nearest_to_id`.** Lets the gateway fetch the seed
  vector cheaply but still re-queries — keeps the two-hop shape and ships the
  vector. Solves the lesser half; leaves the vector search at the edge.
- **Materialize a k-NN graph at index time.** Precompute neighbours per row. Far
  heavier (storage + staleness on every upsert) than seeding ANN on demand; the
  ANN index already answers the query. Rejected for v1.
- **Do nothing.** Leaves `shop`'s recommendations on the two-hop emulation
  permanently. Rejected — recommend is a first-class surface and the seed lives
  here.

## Fork delta

Pure **additive engine capability** on a hard fork — no upstream PR (`AGENTS.md`
§ "This is a hard fork"). Read-path only: a new request field, an optional read
route, and a `seed_vector` helper reusing the existing id-predicate machinery. No
schema, index, or storage delta to track against a cherry-pick.

## References

- `crates/hevsearch-api/src/lib.rs:54-140` — the read/admin route table (no
  `GET /ns/{ns}/doc/{id}` today; `/query` and `/list` present).
- `crates/hevsearch-core/src/manager.rs:1488` (`.nearest_to`), `:1511`
  (`.only_if` prefilter), `:1506` (`.limit`), `:1456-1467` (vector projection),
  `:1182` (`delete_ids` — the id-predicate pattern to reuse), `:163`
  (`RowId::to_sql_literal`), `:1806` (`/list` ordered scan).
- `crates/hevsearch-core/src/result.rs:18` (`RowId::U64|String`), `:42`
  (`QueryResult`), `:131` (`ListRow` always carries `vector`), `:200`
  (`RowIdType`).
- engine RFC 0005 — arbitrary (string) row ids; the seed id is typed like any id.
- engine RFC 0003 — per-row delete; the `id IN (…)` / `to_sql_literal` sibling.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — edge twin; the
  `kind: search` backend and its `nearest_to_id` mapping.
- `CLAUDE.md` § "Engine (keep) vs edge (shed)" — "the document cache adds
  point-fetch-by-id, which the engine lacks. Complementary, not redundant"; the
  "don't make Layer reimplement what the engine owns" rule.
