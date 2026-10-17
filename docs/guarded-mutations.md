# Phrase and guarded mutation primitives

These are internal engine capabilities; Layer owns wire
translation, authorization, source-authoritative effect capture and CDC receipts.
They do not grant permission to replay an unknown write or change read routing.

`phrase_matches(text, phrase)` is available inside `/aggregate` SQL. It applies
Unicode simple lowercase and collapses Unicode whitespace, then tests a contiguous
substring. Punctuation and accents stay intact. Empty/null phrases or null text
return false. It does not require word boundaries, join text across rows, use a
full-text tokenizer, change relevance scores or require a full-text reindex.
Place a bounded ordered candidate CTE before the predicate when preserving an
application's candidate-window/truncation policy. It runs as a native DataFusion
filter over projected Lance batches, not an application corpus download.

The engine provides these write surfaces:

- `/ns/{namespace}/patch`: atomic field changes to a bounded set of typed IDs,
  optional current-value predicate, exact affected IDs/count and commit version.
  Existing attributes and vectors remain intact unless explicitly assigned.
- `/ns/{namespace}/merge`: full postimage replacement under an atomic
  `target`/`source` SQL predicate, with explicit insert-if-missing behavior.
  It reports native inserted/updated/skipped counts and the data commit version.

Declare the namespace profile/columns before these mutations. Schema changes and
row changes are separate operations; no multi-operation transaction is promised.
Guards use engine-owned native storage transactions, not a read/merge/upsert loop.
Version/epoch fields belong to the caller; no global LSN, auth model, idempotency
journal, automatic replay or embedding producer is added to the engine.

A storage/transport failure can leave the effect unknown. A timeout or HTTP500
must never be interpreted as a proven noncommit. Explicit conflicts indicate
rejected storage transactions; client retry policy and terminal-drain evidence
remain Layer's responsibility. Engine commit acknowledgement and search/index
readiness remain separate observations.

## Requests and outcomes

Both endpoints accept 1–100 unique typed IDs, at most 1 MiB of serialized input,
and `timeout_ms` in 1–30000 (default 5000). They require an existing declared
profile and compatible columns; they never create a missing namespace or infer
new attribute columns. Unknown JSON fields are refused by deserialization.

```json
{"rows":[{"id":"a","set":{"summary":"updated","version":2}}],"condition":"version = 1","timeout_ms":5000}
```

`POST /ns/example/patch` returns
`{"patched":1,"affected_ids":["a"],"version":4}`. Missing IDs and false/NULL
guards are skipped. All rows in a patch must assign the same nonempty field set;
heterogeneous sets are refused before dispatch. Attributes, `text`, and the
profile's `vector`/`vectors` field may be assigned. Typed null attributes and list
elements are retained. Text assignments also refresh derived tokenizer content.
Non-null vector copies preserve Float32 bits. Clearing a vector is refused before
dispatch because the pinned native update encoder can produce unreadable data
([issue #61](https://github.com/hev/search/issues/61)).

```json
{"rows":[{"id":"a","vector":[1,0],"text":"replacement","attributes":{"version":2,"epoch":"one"}}],"condition":"target.epoch = source.epoch AND target.version < source.version","insert_if_missing":true}
```

`POST /ns/example/merge` returns `inserted`, `updated`, `skipped`,
`affected_ids`, and `version`. Rows are complete postimages: omitted attributes,
text and vectors become null, subject to the profile's nullability. For
insert-only behavior use `condition:"false", insert_if_missing:true`; a failed
matched guard never falls through to insertion. New profiles carry native
primary-key conflict metadata. Guarded insertion into legacy tables lacking
that metadata is refused; this change does not silently migrate stored data.

Guards must be boolean immutable row predicates. Subqueries, aggregates, windows,
variables, volatile/stable functions and unsupported native expressions are
refused. Supported guard functions are `coalesce` and `make_array`; SQL NULL
semantics apply, so use explicit `IS NULL` branches when appropriate.
Raw legacy strings and typed lists retain their declared types; the engine does
not normalize legacy encodings in the guard. Patch predicates use unqualified
current columns; merge predicates use `target` and `source` qualifiers.

## Storage guarantees and limits

Each operation refreshes a request-owned latest snapshot, validates all input and
computes at most 100 affected IDs. A patch uses native conditional UpdateBuilder
expressions; it never downloads existing rows to manufacture postimages. A merge
uses native conditional MergeInsert. Both execute one storage transaction with
semantic retries disabled. Concurrent conflicts return HTTP409 with
`{"error":"mutation_conflict","outcome":"rejected"}`. Exact affected IDs and
native counts are cross-checked before a success response. No-op responses name
the read snapshot and have no write dispatch.

Failures after dispatch and post-dispatch deadlines return HTTP503 with
`{"error":"mutation_outcome_unknown","outcome":"unknown"}`. Storage details
stay in server logs. Pre-dispatch validation/deadline failures have no mutation
effects. Handles are evicted even on unknown outcomes; result caches are keyed by
persistent table version. A process crash or transport loss still requires caller
reconciliation. There is no durable engine receipt or automatic replay.

Write operations do not expose an EXPLAIN endpoint. Pinned Lance cannot explain
all partial-schema/scalar-index merge paths, and its conditional partial-schema
merge is unsuitable here. Native read execution plans and post-execution metrics
remain available through `/aggregate`. This API does not promise transactions
across namespaces, schema changes, or a write plus a subsequent read.
