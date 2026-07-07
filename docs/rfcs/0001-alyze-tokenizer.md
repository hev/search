# RFC 0001: alyze tokenizer for full-text search

Tracking issue: [#5](https://github.com/hev/search/issues/5)

> **Status:** implemented (Option A, #5). The engine pins `alyze = "=0.1.5"` and
> owns the linguistics: `crates/hevsearch-core/src/analyzer.rs` is the single
> `word_v4` analyzer shared by the write path (the reserved `text_tok` column,
> derived on upsert/import) and the query path; the FTS index is built over
> `text_tok` with the passthrough whitespace builder, the analyzer id is
> recorded in the table's schema metadata (`hevsearch.fts_analyzer`), and
> `POST /ns/{ns}/fts-index` doubles as the explicit backfill/reindex path for
> pre-existing namespaces. Open questions resolved: shadow column (yes),
> phrase/positions (deferred), token bound 39 bytes (mirrors the gateway
> policy), backfill (explicit endpoint), default-on (no flag — legacy tables
> keep serving their old index until reindexed).
>
> **The first RFC in the engine's own series.** Per
> the user's instruction (2026-06-28), engine-specific RFCs are now collected
> here in `docs/rfcs/`, numbered independently of Layer's `../layer/docs/rfcs/`
> series. Layer's RFC 0086 (`kind: search`) is the *edge* view of this engine;
> this is the first *engine* RFC, and it is about the moat — search quality.
>
> **The proposal:** replace LanceDB's built-in FTS analyzer with
> [**alyze**](https://github.com/turbopuffer/alyze) (turbopuffer's `word_v4`
> tokenizer), run as an **external analyzer in the engine** — pre-tokenize on the
> write path and the query path, hand LanceDB a passthrough whitespace tokenizer.
> Tokenization is **owned inside `lance-index`** and selected by *name* from a
> closed match arm, so alyze cannot be injected into the inverted index directly;
> the external-analyzer path is the one that keeps us on pinned LanceDB.
>
> **Why it matters beyond "a nicer tokenizer":** Layer fronts this engine with a
> **Turbopuffer-shaped wire** (RFC 0086), and Turbopuffer's own BM25 is tokenized
> by `word_v4` — i.e. by alyze. Adopting alyze makes the engine's lexical
> behavior **match what a Turbopuffer client already expects**. This is a
> wire-fidelity argument, not just a relevance tweak — and it lands squarely on
> the **engine** side of the engine/edge split (`CLAUDE.md`), so it belongs here.
> It is **additive engine capability**, and on a hard fork that means it lands
> here and stays here: `word_v4` parity exists to match the Turbopuffer wire Layer
> fronts the engine with, so it is **Layer-opinionated** engine work, not a
> generically-upstreamable tokenizer. No upstream PR (`AGENTS.md` § "This is a
> hard fork").

## Summary

Today the engine does no tokenization of its own. Full-text search is handled
entirely by LanceDB's inverted index, driven with `FtsIndexBuilder::default()`,
which resolves to `lance-tokenizer`'s tantivy-derived analyzer (simple
tokenizer + lowercase + English Snowball stemming + English stop-words + ASCII
folding). It is hardcoded and unexposed.

This RFC proposes moving tokenization **into the engine** behind an `alyze`
analyzer that runs on both the write path and the query path, with LanceDB
reconfigured as a **passthrough** (`base_tokenizer = "whitespace"`, all built-in
filters off). LanceDB keeps doing BM25 scoring and posting lists; alyze owns all
the linguistics. The result is `word_v4` behavior — parity with Turbopuffer's
own lexical layer, which is the dialect Layer translates onto this engine.

## Background: what tokenizes FTS today

The engine carries **no application-level tokenizer**. The relevant code is two
calls into LanceDB:

- **Index build** — `crates/hevsearch-core/src/manager.rs:1518`:
  ```rust
  tbl.create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
  ```
- **Query** — the query string is handed straight to LanceDB with no
  preprocessing: `manager.rs:1388` (hybrid leg) and `manager.rs:1408` (FTS-only),
  both `FullTextSearchQuery::new(text)`.

`FtsIndexBuilder` is a re-export of `lance_index::scalar::InvertedIndexParams`.
Its `default()` is **not** the bare whitespace splitter it looks like — the real
defaults (lance-index 6.0.0, `scalar/inverted/tokenizer.rs:205`) are:

| setting | default |
|---|---|
| `base_tokenizer` | `simple` (Unicode word + punctuation split) |
| `lower_case` | **true** |
| `stem` | **true** (English Snowball) |
| `remove_stop_words` | **true** (English) |
| `ascii_folding` | **true** |
| `max_token_length` | 40 |
| `with_position` | false |

So the engine ships an opinionated English analyzer today — it is simply *not
ours to tune*, because it is `default()`.

The `text` column the index is built over is a plain nullable `Utf8` field
(`manager.rs:607`) and a reserved column name (`manager.rs:108`).

## The constraint: tokenization lives inside LanceDB

alyze is a Rust crate that produces a token stream. The natural instinct is to
register it as the inverted index's tokenizer. **That hook does not exist.** The
on-disk posting lists are built inside `lance-index`, and the tokenizer is chosen
by **string name** from a closed match arm
(`lance-index-6.0.0/src/scalar/inverted/tokenizer.rs:384`):

```rust
match self.base_tokenizer.as_str() {
    "simple"     => ...,
    "whitespace" => ...,
    "raw"        => ...,
    "ngram"      => ...,
    s if s.starts_with("lindera/") => ...,
    s if s.starts_with("jieba/")   => ...,
    _ => Err(Error::invalid_input(...)),
}
```

There is no registration / plug-in path for an external crate. You get the
built-in tantivy-derived analyzers or nothing. That fact picks the design for us.

## Design — Option A: alyze as an external analyzer (recommended)

Run alyze in the engine, on both sides of the index, and reduce LanceDB to a
dumb whitespace splitter so it adds no linguistics of its own.

```
WRITE   text ──alyze.analyze()──► "tok1 tok2 …" ──► text_tok column ──┐
                                                                       ├─► LanceDB FTS
QUERY   q    ──alyze.analyze()──► "tok1 tok2 …" ──► FullTextSearchQuery┘   (whitespace,
                                                                            filters off,
                                                                            BM25 only)
```

Concretely:

1. **Write path.** When a row's `text` is upserted, run it through the engine's
   alyze analyzer and store the space-joined tokens in a new reserved column
   `text_tok` (Utf8). `text` is retained verbatim for return payloads /
   highlighting; `text_tok` is the *indexed* surface.
2. **Index build.** Build the FTS index over `text_tok` with a passthrough
   builder so LanceDB re-tokenizes only on whitespace and applies no further
   analysis:
   ```rust
   FtsIndexBuilder::default()
       .base_tokenizer("whitespace".into())
       .lower_case(false)
       .stem(false)
       .remove_stop_words(false)
       .ascii_folding(false)
       .max_token_length(None)   // alyze already bounds token length
   ```
3. **Query path.** Run the query string through the *same* alyze analyzer,
   space-join, and pass the result to `FullTextSearchQuery::new(...)`.

The load-bearing invariant: **write-path and query-path analysis must stay
byte-for-byte identical** — same alyze version, same pipeline configuration. A
drift between them silently tanks recall. The analyzer should therefore be a
single owned construction in `hevsearch-core`, shared by both paths, with its
configuration captured in the index manifest so a rebuild can't desync from
writes.

alyze's pipeline (UAX #29 DFA tokenizer; optional lowercase, ASCII case-fold,
stemming, stop-word stages) is chained once at engine startup. The default we
ship should be the `word_v4` configuration so the behavior matches Turbopuffer.

### Why this is the right shape

- **Stays on pinned LanceDB/lance.** No transitive fork; the `=0.29.0` / `=6.0.0`
  pins (`Cargo.toml:24`) hold.
- **Engine owns the linguistics.** Tunable, testable, and ours — the moat.
- **Additive.** A new column + an analyzer module + a builder config; nothing
  removed, no API contract broken.

## Open questions

These are deliberately left open for the implementation PR:

- **Shadow column vs. in-place.** `text_tok` (proposed) keeps `text` clean for
  payloads at the cost of one extra Utf8 column and a write-path transform.
  Indexing `text` directly is simpler but loses the verbatim original. Shadow
  column is the recommended default.
- **Positions / phrase queries.** Passthrough whitespace + space-joined tokens
  drops positional phrase search unless we also emit token positions and set
  `with_position(true)`. Decide whether phrase support is in v1 or deferred.
- **Per-namespace configuration.** Ship a single global `word_v4` analyzer first.
  A future per-namespace analyzer choice (language, stemming on/off) is a natural
  follow-on, but it is **engine config, not an edge/tenant feature** — keep it
  here, not in Layer.
- **Migration / reindex.** Existing namespaces have a populated `text` but no
  `text_tok`. Define the backfill (re-derive `text_tok` from `text`, rebuild the
  FTS index) and whether it is automatic on first FTS build or an explicit
  maintenance endpoint (mirrors the `id`/`_ingested_at` backfill rationale at
  `manager.rs:1528`).
- **Default-on vs. opt-in.** Whether `word_v4` becomes the default analyzer or
  ships behind a flag for one release while the eval (below) runs.
- **`max_token_length` parity.** Confirm alyze's token-length bound vs. LanceDB's
  current 40 so we don't silently change long-token behavior.

## Alternatives considered

- **B — patch `lance-index` to add an `"alyze"` arm.** Add a branch to
  `build_base_tokenizer` that constructs an alyze-backed `LanceTokenizer`.
  **Rejected:** it forks a *transitive* dependency (lance-index 6.0.0) we pin
  exactly — an unbounded maintenance burden on every `lance` bump. Even though
  `hev/search` is itself a hard fork, forking a pinned transitive dep is the one
  divergence we don't want. It is also strictly more code than Option A for a
  worse maintenance position.
- **C — just expose LanceDB's existing analyzer config.** `InvertedIndexParams`
  already has setters for `base_tokenizer`, `language`, `stem`,
  `remove_stop_words`, `ascii_folding`, `ngram_*`. Surfacing these as index
  options is a real, cheap win and worth doing regardless. But it does **not**
  get `word_v4` parity with Turbopuffer — the tantivy-derived analyzer is a
  different tokenizer with different boundaries. C is a complement to A, not a
  substitute.
- **D — do nothing.** Keep `FtsIndexBuilder::default()`. Leaves the engine's
  lexical behavior diverging from the Turbopuffer dialect Layer translates onto
  it, and leaves the analyzer untunable. Rejected as the baseline this RFC exists
  to move off.

## Compatibility, licensing, distribution

- **alyze** is published on crates.io (`alyze 0.1.5`, 2026-06-18, MIT,
  maintained by turbopuffer). MIT is compatible with this repo's Apache-2.0 — an
  MIT dependency in an Apache-2.0 crate is fine.
- Pin it **exactly** per project convention (the same discipline as the
  `lancedb`/`lance` pins in `Cargo.toml`): `alyze = "=0.1.5"`. A bump is a
  read-the-changelog event, because the analyzer config *is* the index format —
  changing alyze's tokenization across an existing index requires a reindex.
- alyze pulls `rust-stemmers` and a turbopuffer ICU-properties crate; `rust-version`
  1.85, matching this workspace.

## Evaluation

The engine already has the harness to prove this lands without regressing
quality:

- **Unit/integration parity.** The existing FTS integration test
  (`crates/hevsearch-api/tests/api_fts.rs`, the three query modes) must stay green
  through the swap. Add a direct analyzer test asserting alyze output equals the
  expected `word_v4` token stream for a fixed corpus, and a write↔query lockstep
  test.
- **Retrieval quality.** Reuse the BEIR/FiQA harness in `bench/`
  (`bench/results/beir_multivector_raw/`, the FiQA quality runs) to compare nDCG/recall
  on the lexical leg, default analyzer vs. alyze, before flipping the default.
- **Turbopuffer parity (the thesis).** Where feasible, assert the engine's
  tokenization matches Turbopuffer's `word_v4` on a shared corpus — that is the
  property the whole change is for.

Gate the default flip (Option A phase 2 → on) on the BEIR number, mirroring the
phased-eval discipline the portfolio uses elsewhere.

## Fork delta

`hev/search` is a **hard fork** now (`AGENTS.md` § "This is a hard fork"), so this
change lands here and stays here — **no upstream PR to `gordonmurray/firnflow`.**
The motivation is itself Layer-opinionated: `word_v4` parity exists to match the
Turbopuffer wire Layer fronts the engine with, so an external-analyzer surface
isn't a generic upstream contribution — it's ours. Record the engine deltas (the
`text_tok` reserved column, the passthrough `FtsIndexBuilder`, the pinned `alyze`
dep) so a hand cherry-pick of an upstream fix doesn't fight them.

There is **no subtractive edge removal** here — this is pure engine capability.

## Phasing

1. **Prototype behind a flag + eval.** Wire the alyze analyzer into `hevsearch-core`,
   build a `text_tok`-backed FTS index in a feature-flagged path, and run the
   BEIR/FiQA comparison against the current default. No default change, no API
   change.
2. **Write path + query path + passthrough index.** Make `text_tok` a first-class
   reserved column with the write-path transform, the passthrough
   `FtsIndexBuilder`, and the shared query-path analyzer. Define and ship the
   reindex/backfill path. Flip the default once the eval clears.
3. **Config surface.** Expose analyzer choice (at least the existing LanceDB
   knobs; ideally a named-analyzer selection) on the index-create surface. Stays
   in the fork — no upstream PR.

## References

- alyze — https://github.com/turbopuffer/alyze (crates.io `alyze 0.1.5`, MIT)
- `crates/hevsearch-core/src/manager.rs:1518` — current FTS index build
  (`FtsIndexBuilder::default()`); `:1388`/`:1408` — query paths; `:607` — `text`
  column; `:108` — reserved column list.
- `lance-index-6.0.0/src/scalar/inverted/tokenizer.rs:205` — real analyzer
  defaults; `:384` — the closed `base_tokenizer` match arm (the constraint).
- `bench/` — BEIR/FiQA quality harness for the eval gate.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the edge view: Layer
  operates this engine behind a Turbopuffer-shaped wire (the parity motivation).
- `CLAUDE.md` § "Engine (keep) vs edge (shed)", `AGENTS.md` § "Change protocol".
