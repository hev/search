# Phrase and guarded mutation primitives

Work in progress. These are internal engine capabilities; Layer owns wire
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

The following write surfaces are being implemented:

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
