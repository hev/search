# RFC 0005: Arbitrary string row ids

Tracking issue: [hev/search#20](https://github.com/hev/search/issues/20)

> **Status:** Accepted (2026-07-08). **Additive engine capability — a
> foundational impedance fix.** This is engine-owned identity, not edge-owned
> auth/tenancy/state. The hard fork lands and carries this capability locally;
> there is no upstream PR.

## Summary

Rows may be keyed by either the existing `u64` id or by an arbitrary UTF-8 string
id. The id type is fixed per namespace at first write/import, matching the
engine's existing first-write shape rule for vector kind and dimension. Existing
numeric-id namespaces and callers remain on the `u64` path; string-keyed
namespaces become first-class, so Layer can send its document ids directly
without maintaining a surrogate id map.

This RFC is the foundational impedance fix for moving the demo family onto
`kind: search`: ids such as `asin-B08N5WRWNW`, `ticket-4117`, and openFDA set ids
must survive upsert, query, list, facet, delete, and import unchanged.

## Why This Belongs In The Engine

The row id is the table identity:

| Site | Current engine reality | Role |
|---|---|---|
| Result model | `RowId::{U64, String}` and `RowIdType::{U64, String}` in `crates/hevsearch-core/src/result.rs` | in-memory, cache, and JSON result shape |
| Table schema | `schema_for_kind(..., id_type, ...)` chooses `UInt64` or `Utf8` for the non-null `id` field | on-disk primary column |
| Upsert merge key | `merge_insert(&["id"])` | latest-write-wins identity |
| Auto id index | first write builds the `id` BTree best-effort | merge-insert lookup speed |
| List cursor | `encode_list_cursor(ts, RowId)` / `decode_list_cursor` | value-based pagination tiebreak |
| Import schema | Arrow `id` accepts `UInt64` or `Utf8` | bulk first-load identity |
| Wire docs | `docs/api.html` documents `id_type`, string delete ids, and fixed namespace shape | public contract |

A gateway-side map from string ids to numeric ids was rejected. Hashing can
collide and corrupt latest-write-wins. A sequence allocator plus reverse map is a
new stateful gateway component on the hot path. Both make Layer reimplement
engine-owned identity. The clean boundary is: Layer supplies the caller's id; the
engine stores, indexes, paginates, deletes, and returns that id.

## Accepted Design

### Id type is fixed per namespace

- First JSON upsert into a fresh namespace fixes `id_type` from `rows[].id`:
  a JSON number is `u64`, and a JSON string is `string`.
- First Arrow import into a fresh namespace fixes `id_type` from the `id` column:
  `UInt64` is `u64`, and `Utf8` is `string`.
- Subsequent writes/imports with the wrong id type return `400`, the same shape
  as vector kind/dimension mismatches.
- `GET /ns/{namespace}` reports `id_type` alongside `kind`, `vector_dim`, and the
  distance metric.
- There is no in-place id-type migration. Changing id type is a
  delete-and-recreate operation, just like changing vector kind/dimension.

### Row representation

The engine uses a single id enum:

```rust
pub enum RowId {
    U64(u64),
    String(String),
}

pub enum RowIdType {
    U64,
    String,
}
```

`serde(untagged)` preserves the wire shape: numeric ids serialize as JSON
numbers and string ids serialize as JSON strings. Query results, list rows,
namespace metadata, delete requests, and cache payloads all carry `RowId` rather
than lossy stringification.

### Schema and write path

`schema_for_kind` receives the namespace `id_type` and creates the `id` column as
`UInt64` for numeric namespaces or `Utf8` for string namespaces. Upsert still uses
`merge_insert(&["id"])`, so latest-write-wins semantics are identical across id
types. Duplicate ids within one upsert request are rejected before any write.

The first write to a fresh namespace still attempts to build a BTree index on
`id`. The accepted design relies on LanceDB supporting a BTree over `Utf8`, and
the implementation keeps the build best-effort: if the post-write index build
fails, the rows are already durable and the operator can rebuild the scalar index
later.

### Cursor format

List pagination remains value-based on `(_ingested_at, id)` and remains opaque to
clients. The accepted cursor format is:

- `v1:u:{timestamp_hex}:{u64_id_hex}` for numeric ids.
- `v1:s:{timestamp_hex}:{hex_utf8_id_bytes}` for string ids.
- The legacy 32-hex-character numeric cursor remains decodable for compatibility.

Clients must continue to round-trip the cursor verbatim.

### Import path

`/import` accepts Arrow streams whose `id` column is either non-null `UInt64` or
non-null `Utf8`. A fresh namespace inherits that id type; an existing namespace
must match it. Schema validation still rejects missing, extra, or mistyped
columns before work starts.

Import remains append-only: repeated ids create additional rows. Callers use
`/import` for first-loads or known-new ids and `/upsert` for idempotent updates.

### Delete and predicates

Row delete by id accepts numeric or string ids matching the namespace `id_type`.
The predicate builder uses `RowId::to_sql_literal`, so string ids are quoted and
escaped correctly before compiling `id IN (...)`.

Free-form filters keep using the DataFusion/Lance predicate dialect. For string
id namespaces, callers quote ids in filters exactly as they quote any other
string scalar value.

### Multivector compatibility

Id type and vector kind are independent namespace properties. String ids work for
both single-vector and multivector namespaces, including Arrow import schemas
with `id: Utf8` plus `vectors: List<FixedSizeList<Float32, dim>>`.

## Edge Mapping

Layer maps its string document ids directly to string-id search namespaces. There
is no hash, sequence allocator, reverse map, or gateway state. `nearest_to_id`,
batch fetch, delete-by-id, and result echoing can all use the same document id
once the corresponding Layer-side endpoints are wired.

Until a namespace is string-id capable, the only honest alternatives are:
restrict that namespace to integer ids, or make Layer maintain a stateful
surrogate map. This RFC accepts the engine fix instead.

## Testing And Evidence

Current code and tests resolve the design questions as follows:

| Question | Resolution | Evidence |
|---|---|---|
| Enum vs branch | Resolved: use `RowId` / `RowIdType` enums end to end. | `crates/hevsearch-core/src/result.rs` |
| Schema choice | Resolved: `schema_for_kind` creates `id` as `UInt64` or `Utf8`. | `crates/hevsearch-core/src/manager.rs` |
| Mixed-type writes | Resolved: fixed namespace `id_type`; wrong-type upsert/import/delete returns invalid request. | `upsert`, `import_arrow_with_distance_metric`, `delete_ids` |
| Cursor re-encode | Resolved: versioned `v1:u` / `v1:s` cursor with legacy numeric decode. | `encode_list_cursor`, `decode_list_cursor`, cursor unit tests |
| Import | Resolved: Arrow `id` may be `UInt64` or `Utf8`; existing namespace must match. | `validate_arrow_import_schema` |
| Delete quoting | Resolved: delete-by-id builds `id IN (...)` from `RowId::to_sql_literal`. | `delete_ids`; string quote regression test |
| Multivector + string id | Resolved: id type and vector kind are independent; import validation covers string-id multivector schemas. | `validate_arrow_import_schema` test with `id: Utf8` and `vectors` |

Coverage that must remain present:

- String ids round-trip through upsert, query, list pagination, facet, and
  namespace info.
- A numeric id written to a string-id namespace returns `400`.
- String delete ids quote literals correctly, including embedded apostrophes.
- Import accepts `id: Utf8` and rejects mismatched id types against existing
  namespaces.
- Existing numeric-id tests stay green unchanged.

## Open Question For Review

- **String id length bound.** Earlier text proposed a bounded max byte length for
  index friendliness, but current code does not appear to enforce one. Accept the
  unbounded `Utf8` behavior, or file a follow-up implementation issue to add a
  specific byte limit and API error.

## Alternatives Considered

- **Gateway-side surrogate map, hash, or sequence.** Rejected. Hash collisions
  corrupt identity; a sequence allocator and reverse map add gateway state and
  duplicate engine-owned storage semantics.
- **Always store ids as `Utf8`.** Rejected. It is simpler type-wise but needlessly
  changes the storage/performance profile and wire contract for existing numeric
  namespaces.
- **Add `_external_id` while keeping numeric `id` primary.** Rejected. If
  latest-write-wins keys on `_external_id`, then numeric `id` is vestigial; if it
  does not, duplicate external ids leak.
- **Do nothing.** Rejected. The demo family's string-keyed corpora cannot use the
  owned search engine faithfully without an edge-side identity hack.

## Fork Delta

This is a pure additive engine capability in the hard fork. Keep it local. Manual
upstream cherry-picks must not silently remove `RowId`, `RowIdType`, string
`id_type` schema handling, string-id cursors, or string delete/import behavior.

## References

- [hev/search#20](https://github.com/hev/search/issues/20) — docs gate and
  acceptance spec.
- `crates/hevsearch-core/src/result.rs` — `RowId`, `RowIdType`, query/list/info
  result structs.
- `crates/hevsearch-core/src/manager.rs` — schema selection, upsert merge key,
  id index build, import validation, delete predicates, cursors.
- `crates/hevsearch-core/tests/manager_local_fs.rs` — string id round-trip and
  delete quoting coverage.
- `crates/hevsearch-api/tests/api_delete.rs` — API-level string delete coverage.
- `docs/api.html` — public wire documentation for `id_type`, import, list
  cursors, and delete ids.
- `docs/rfcs/0003-row-delete.md` — row delete dependency.
- `docs/rfcs/0007-point-fetch-and-nearest-to-id.md` — downstream `nearest_to_id`
  dependency on string ids.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — Layer-side search
  backend direction.
- `CLAUDE.md` and `AGENTS.md` — engine/edge split and hard-fork posture.
