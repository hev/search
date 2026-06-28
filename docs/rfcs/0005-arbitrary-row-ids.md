# RFC 0005: Arbitrary (string) row ids

Tracking issue: _TBD_

> **Status:** draft, proposal. **Additive engine capability — a foundational
> impedance fix.** The engine keys every row by **`u64`** (`Row.id: u64`,
> `manager.rs:344`; `Field::new("id", DataType::UInt64, false)`, `:605`). Layer's
> document model — and the demo family it fronts — is **string-keyed**:
> `asin-B08N5WRWNW`, `ticket-4117`, openFDA set ids. There is no faithful way to
> carry a string id through a `search` namespace today. This sits on the **engine**
> side of the split (it is the table's primary-key type — storage/index, not edge
> auth/tenancy), so the fix is here. Hard fork: lands here, stays here, no upstream
> PR (`AGENTS.md` § "This is a hard fork"). The edge twin is
> `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md`.

## Summary

Let a namespace key rows by an **arbitrary string** (`Utf8`) id, not only `u64`.
The id type is **fixed per namespace at first write** — exactly the pattern the
engine already uses to fix vector kind and dimension at first upsert — so existing
`u64` namespaces are untouched and string-keyed namespaces become a first-class
option. This removes the need for any gateway-side surrogate id map and lets
Layer's string document model land on the engine unchanged.

## Background: `u64` is wired through everything

The id is `u64` end to end, not just in the upsert body:

| Site | Code | Role |
|---|---|---|
| Row struct | `manager.rs:344` (`pub id: u64`) | in-memory + result shape |
| Table schema | `manager.rs:605` (`Field::new("id", DataType::UInt64, false)`) | on-disk primary column, non-null |
| Upsert merge key | `manager.rs:865` (`merge_insert(&["id"])`) | latest-write-wins identity |
| Auto id index | `manager.rs:926` (BTree on `id`) | merge-insert lookup speed |
| List cursor | `manager.rs:1972` `encode_list_cursor(ts_micros: i64, id: u64)`; decode `:1987` (`u64::from_str_radix`) | the `(_ingested_at, id)` value-based pagination tiebreak |
| Wire | `docs/api.html` (`rows[].id` is `u64`; results `id` is `u64`) | the documented contract |

So "support string ids" is not a parser tweak — it is the table's primary-key
type, and it touches the schema, the merge identity, the scalar index, the
pagination cursor, and every result body.

## Why this is the right layer (engine, not a gateway map)

A tempting edge workaround is for Layer to hash/sequence each string id to a `u64`
and keep a reverse map (u64 → string) to reconstruct results. Rejected as the
primary design:

- **Hashing collides.** Two strings → one `u64` silently corrupts latest-write-wins
  (one doc overwrites another). A collision-free sequence needs a persistent
  allocator + reverse map — a stateful component on the gateway hot path, which the
  stateless-gateway frame (`../layer/CLAUDE.md`) pushes back on.
- **It reimplements identity the engine should own.** The primary key is an engine
  concept; a gateway-side surrogate map is exactly the "don't make Layer
  reimplement what the engine owns" smell (`CLAUDE.md`). The clean fix is the
  engine accepting the id the caller actually has.

So the id type belongs in the engine. Layer stays the edge.

## Design

### Id type fixed per namespace at first write

Mirror the existing first-write-fixes-the-shape rule (vector kind + dim are fixed
by the first upsert into a fresh namespace — `manager.rs` `schema_for_kind`,
`docs/api.html`). Add **id type** to that fixed shape:

- First upsert/import into a fresh namespace fixes `id_type ∈ { u64, string }`
  from the JSON type of `rows[].id` (a number ⇒ `u64`, a string ⇒ `Utf8`).
- Subsequent writes in the wrong id type → `400`, same as a vector-shape mismatch.
- Default and back-compat: an integer id keeps the `u64` column exactly as today;
  nothing changes for existing namespaces or integer-id callers.
- `GET /ns/{ns}` reports `id_type` alongside `kind`/`vector_dim` so callers and the
  operator can see it.

### What changes per site

1. **Schema** (`manager.rs:605`, `schema_for_kind`): the `id` field becomes
   `Utf8` (non-null) for string namespaces; `UInt64` otherwise. String ids should
   carry a bounded max length (e.g. reject ids over N bytes) so they stay
   index-friendly — define N in the PR.
2. **Row / result type** (`manager.rs:344` and the query/list/facet response
   builders): `id: u64` becomes an id enum (`U64(u64) | Str(String)`) or the
   handlers branch on `id_type`. Result JSON echoes the id in the type it was
   written.
3. **Merge key** (`manager.rs:865`): `merge_insert(&["id"])` already keys by the
   `id` column; it works on `Utf8` unchanged — latest-write-wins by string id.
4. **Auto id BTree** (`manager.rs:926`): a BTree over a `Utf8` column is supported;
   keep building it on first write. Lookups stay indexed.
5. **List cursor** (`manager.rs:1972`/`:1987`): today the cursor packs
   `(i64 ts, u64 id)` as fixed-width hex. A `Utf8` id breaks the fixed-width pack;
   re-encode as a length-prefixed / base64 `(ts, id_bytes)` token. The cursor stays
   opaque (`docs/api.html`: "Format is implementation-defined — do not parse"), so
   the encoding can change freely; only the engine reads it.
6. **Delete predicates** (engine RFC 0003): `id IN (…)` must quote string ids
   (`id IN ('asin-1','asin-2')`). The predicate builder branches on `id_type`.
7. **Validation**: a string id is arbitrary user text (not subject to the
   attribute-name "SQL-friendly identifier" rule, which governs *column* names);
   only the length bound and non-null apply. Duplicate ids within one request stay
   a `400`, as for `u64`.

### Import (Arrow) path

`/import`'s Arrow schema requires `id: UInt64` today (`docs/api.html`). Extend it to
accept `id: Utf8` for string namespaces, fixed by / checked against the namespace's
`id_type`, with the same "extra/mistyped column ⇒ 400 before work starts" rule.

## Edge mapping (how Layer uses this)

Layer's string document ids map **directly** to a `string`-id namespace — no
surrogate, no reverse map, no gateway state. `nearest_to_id` (which takes string
document ids), batch fetch, and result `id` echoes all carry the caller's strings
through unchanged. Until this lands, the honest options for a `search` namespace
are: restrict it to integer ids, or have Layer maintain a (stateful, lossy-if-hashed)
id map — which is the current matrix constraint
(`../layer/site/src/content/docs/kubernetes/store-support.mdx`, "Document id type:
integer (`u64`) only").

## Open questions (for the implementation PR)

- **Enum vs. branch.** Represent the id as a Rust enum end to end, or branch on
  `id_type` at the boundaries? Enum is cleaner; measure the churn.
- **Max id length.** Pick the byte bound (BTree/index friendliness vs. real-world
  ids like content hashes).
- **Cursor re-encode.** Settle the new opaque `(ts, id)` token format; keep old
  `u64` cursors decodable (or accept that a format bump invalidates in-flight
  cursors — they are short-lived).
- **Mixed-type migration.** No in-place id-type change on an existing namespace
  (it is fixed, like vector kind); a switch is delete-and-recreate. Confirm that is
  acceptable.
- **Multivector + string id.** Orthogonal (id type and vector kind are independent
  fixed properties); confirm both combine cleanly.

## Testing

- **Integration** (`crates/hevsearch-api/tests/`): a fresh namespace seeded with
  string ids round-trips through upsert → query → list → facet with ids echoed as
  strings; a wrong-type id on a fixed namespace → `400`; the auto BTree builds; the
  list cursor paginates correctly across a string-id namespace; delete-by-id
  (RFC 0003) quotes correctly. Existing `u64` tests stay green unchanged.
- **Import**: an Arrow stream with `id: Utf8` ingests into a string namespace and
  is rejected against a `u64` namespace.

## Alternatives considered

- **Gateway-side surrogate map (hash or sequence).** Rejected — collisions corrupt
  latest-write-wins (hash) or require a stateful allocator + reverse map on the
  gateway hot path (sequence), reimplementing engine-owned identity (`CLAUDE.md`).
- **Always `Utf8` (drop `u64`).** Simpler type-wise but a needless perf/space cost
  for integer-id callers and a breaking change for every existing namespace.
  Fixed-per-namespace keeps the fast `u64` path and adds the string path.
- **A second reserved `_external_id` column, `u64` stays primary.** Then
  latest-write-wins must key on `_external_id` anyway (or duplicates leak), so the
  `u64` is a vestigial surrogate — collapses into "make the primary key the
  caller's id," i.e. this RFC.
- **Do nothing.** Leaves the demo family's string-keyed corpora unable to use the
  owned engine without a lossy edge hack. Rejected.

## Fork delta

Pure **additive engine capability** on a hard fork — no upstream PR (`AGENTS.md`
§ "This is a hard fork"). Record the schema/id-type deltas so a hand cherry-pick of
an upstream change doesn't fight them. No subtractive edge removal.

## References

- `crates/hevsearch-core/src/manager.rs:344` (`Row.id: u64`), `:605` (UInt64 id
  field), `:865` (`merge_insert(&["id"])`), `:926` (auto id BTree), `:1972`/`:1987`
  (list cursor `(ts, u64 id)` encode/decode), `schema_for_kind` (`:590`).
- `docs/api.html` — `rows[].id` / results `id` as `u64`; `/import` Arrow `id:
  UInt64`; the opaque-cursor and first-write-fixes-shape contracts.
- engine RFC 0003 — per-row delete; `id IN (…)` must quote string ids.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — edge twin; the
  string document model and the current `u64`-only constraint.
- `CLAUDE.md` § "What the engine is NOT (Layer owns it)" / "Engine (keep) vs edge
  (shed)"; `../layer/CLAUDE.md` § "Stateless Gateway Frame"; `AGENTS.md`
  § "The engine/edge test".
