# Native bounded reads and aggregates

`POST /aggregate` is an internal engine API over native Lance 6 / DataFusion
53.1 table providers. Layer owns authorization, namespace allowlists and its
public API. Callers of Layer must use Layer's supported API, never this engine
endpoint directly. This adds no embedding, CDC, replay or retention policy.

```json
{
  "tables": {"sessions": "example-sessions", "evals": "example-evals"},
  "sql": "SELECT model, count(*) AS sessions, sum(CAST(cost AS DECIMAL(24,6))) AS spend FROM sessions GROUP BY model ORDER BY model",
  "max_rows": 100,
  "max_bytes": 1048576,
  "timeout_ms": 10000,
  "explain_only": false
}
```

Aliases are request-local lower-case identifiers; bindings accept validated
namespace names, never paths or external tables. One to eight bindings are
required. SQL supports projections, compound ordering, scalar/list predicates,
window functions, semi/anti/left joins, UNNEST and grouped reductions through
DataFusion. DDL, DML, COPY, SET and multiple statements are refused. A missing
namespace or unsupported SQL/type fails explicitly; it is never an empty table.

Results include `rows`, `snapshots` (alias, namespace and Lance version),
`logical_plan`, `physical_plan`, `elapsed_ms`, `result_bytes`, `executed`.
The actual physical plan after execution includes operator metrics (ANALYZE).
`explain_only:true` plans without executing and returns no rows. Metrics are
what the pinned operators expose: unavailable physical I/O/CPU/spill counters
are unknown, never invented zeros. Cross-table versions are individually
captured and are not a common transaction or a CDC convergence boundary.
Optional `versions:{"example-sessions":123}` pins physical namespaces for
repeatable chart/page requests; every alias of that namespace shares its
snapshot. An unavailable version fails rather than falling back. Retention
remains the caller’s responsibility; this endpoint does not pin snapshots across
requests or prevent compaction.

Source batches stream into DataFusion operators. Rust only encodes bounded
result batches; it does not collect source rows to fold aggregates. The final
result has a sentinel row limit: exceeding `max_rows` fails rather than returning
partial totals. Bounds are 1–10000 rows, 1–8 MiB JSON rows, and 1–60000 ms.
The per-request DataFusion pool is 256 MiB with 512 MiB spill and four execution
partitions. DataFusion/Lance do not account every allocation in this pool;
these are operator budgets, not a process RSS guarantee. The deadline cancels
the future/stream cooperatively, not by killing a shared process. Expensive
queries can scan the relevant corpus: bounded output does not imply bounded
physical reads. Gateway resource admission and measured scan budgets remain
necessary. All budget failures return 400 with no partial result.

Scalar DECIMAL outputs are exact JSON strings; select DECIMAL for financial
reductions. DOUBLE preserves floating point semantics and is not exact money.
INT64/UINT64 use JSON numbers; clients must preserve integer precision.
SQL NULL is explicit JSON null. Arrays preserve null elements and empty lists.
Use unique output column names; Arrow JSON objects cannot represent duplicate
keys. Do not aggregate financial metrics after expanding prompt arrays.

## Latest state, joins and time

Use a window boundary before predicates on mutable fields. For logical session
identity this ordering selects greatest end, canonical ID, then greatest legacy
ID. Put filters and pagination **outside** `latest`. List ties use end descending,
session ID ascending. Null end sorts last explicitly.

```sql
WITH ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY session_id
    ORDER BY "end" DESC NULLS LAST, (id = session_id) DESC, id DESC
  ) AS rn FROM sessions
), latest AS (SELECT * FROM ranked WHERE rn = 1)
SELECT id, session_id, model FROM latest
WHERE model = 'example' ORDER BY "end" DESC, session_id ASC LIMIT 100
```

Resolve evals by timestamp then ID **before** grade filtering. Join the latest
eval set to latest sessions. A left anti join against latest poor evals includes
ungraded sessions; `grade != 'poor'` on an inner join would lose them.

Reduce spend/tokens/prompts on selected sessions before any UNNEST. Coalesce
each missing/null usage or cost field to zero
before arithmetic when matching an application that decodes missing metrics as
zero; SQL otherwise propagates NULL through addition and can omit a whole row.
Tokens sum input, output, cache read and cache creation. Cache hit is
`sum(cache_read)/NULLIF(sum(input+cache_read+cache_creation),0)` and
cost per prompt is `sum(cost)/NULLIF(sum(prompt_count),0)`; COALESCE these ratios
to zero for empty denominators when the API contract requires it. Coverage is
sessions with nonempty prompt timestamps divided by selected sessions, computed
before expansion. Use an EXISTS/semi join for ANY matching prompt selection,
then UNNEST **all** prompts for the selected sessions. Never keep only the
prompts that qualified the session. Null/empty arrays contribute no prompt bins.

Epoch milliseconds are UTC instants. For IANA heatmaps, attach the timezone
**to the integer epoch**, then convert to wall time:

```sql
to_local_time(arrow_cast(prompt_ms, 'Timestamp(Millisecond, Some("America/New_York"))'))
```

For ISO strings use `CAST(to_timestamp(ts) AS BIGINT)` with
`Timestamp(Nanosecond, Some("America/New_York"))`. Applying AT TIME ZONE directly
to a naive timestamp treats it as local wall time and changes the instant.
The pinned DataFusion version also has a `to_timestamp` planned/runtime type
mismatch when its session timezone is configured; this executor keeps the
default timezone-naive session. Integer epoch conversion avoids both behaviors.
Daily series use UTC session-start epochs, with whole-session cost/usage
attributed to the selected current model. Apply `date_part('dow', ...)` /
`date_part('hour', ...)` to the local result; Sunday is zero.
A repeated DST hour counts both prompts. Invalid timezone names are errors.
The engine does not supply application-specific project-basename rules,
12-bin series recipes, or missing-day zero fill: Layer must express the accepted
contract using these operators and prove it against its frozen source oracle.

## Schema and metadata-first ingestion

`GET /ns/{namespace}/schema` reads the typed persisted profile (404 if missing).
`POST /ns/{namespace}/schema` creates an empty namespace or adds compatible
attribute columns (204). It rejects changes to dimension, kind, ID type, metric,
or any existing attribute type before applying additions.

```json
{
  "dim": 1536,
  "kind": "single",
  "id_type": "string",
  "distance_metric": "cosine",
  "attributes": {"session_id":"utf8", "prompt_ts":"int64_list", "cost":"float64", "embedding_pending":"boolean"}
}
```

Supported attribute types: `int64`, `float64`, `boolean`, `utf8`, `string_list`,
`int64_list`, `float64_list`. Numeric lists allow native timestamp UNNEST.
Empty/null-only values carry no inference signal; declare their types explicitly.
Mixed element types and incompatible schema upgrades fail, preserving source.
All rows retain typed IDs. Schema mutations use Lance commits; no multi-writer
transaction or conditional schema upgrade is claimed.

New namespaces have nullable vectors. After declaring the source vector profile,
an upsert can omit vectors to persist metadata/text first, storing a NULL vector
rather than a fabricated zero embedding. A later authoritative replacement can
supply the exact persisted vector and attributes. Upsert is full replacement,
not patch: omitting attributes/vectors clears them. Missing vectors in a fresh
namespace require an explicit profile or another row with a vector. Legacy
non-nullable vector tables refuse metadata-only writes; no automatic rewrite
of retained data occurs. Vector presence proves storage only, not source version
identity, embedding completion or a full CDC ACK; Layer owns that protocol.

Facets now use native GROUP BY / UNNEST, returning at most top+1 buckets per
field to determine truncation. Null scalars form a bucket; null lists and null
elements do not. Duplicate list values contribute repeatedly. Ties sort by typed
value ascending with null first. Separate fields currently make separate scans.

## Validation and scope

Local-storage Rust tests exercise actual Lance providers and router requests:

```sh
cargo test -p hevsearch-core --test native_aggregate --test service_facet
cargo test -p hevsearch-api --test api_aggregate
```

Use the containerized `./scripts/cargo` equivalent where Docker is available.
These fixtures prove engine primitives, not installed Layer/Kit parity, auth,
CDC durable completion, archive reconciliation, or permission to change routing.
Search fusion/relevance, public SDK mirrors, live archive backfill and source
cleanup are unchanged by this API. Unsupported application contracts must remain
explicit in Layer's capabilities until its exact candidate passes acceptance.

Unpinned reads refresh a request-owned latest manifest, including commits made by
other API processes. Explicit version pins remain historical; aliases for one
namespace share one checkout. A later concurrent commit is outside that request
snapshot, and namespace snapshots are still not a cross-namespace transaction.
