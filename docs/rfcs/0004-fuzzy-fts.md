# RFC 0004: Fuzzy / typo-tolerant full-text search

Tracking issue: hev/search#6

> **Status:** implemented. The `fuzzy.max_edit_distance` control (`0`/`1`/`2`/
> `"auto"`) shipped on `/query` via Option A (Lance `MatchQuery` fuzziness).
> The first wiring handed Lance one **uniform** nonzero distance for the whole
> query, which applied it to every token — 1–2 char tokens expanded to
> essentially the whole term dictionary and every fuzzy query returned the same
> match-all list (hev/search#6, found by the `shelf` cutover). The fix keeps
> Option A but expands **per token** with the length-keyed ladder below
> (1–5 chars exact, 6–8 → d=1, 9+ → d=2; a fixed value caps the ladder) as a
> `BooleanQuery` of per-token `MatchQuery` clauses. Remaining open item: the
> analyzer-parity gap (RFC 0001) — fuzzy operates on the query's token surface,
> and until alyze lands the index side stems/stops with Lance's default
> analyzer, so stemmed terms can sit farther than their surface edit distance.
>
> Original framing (kept for the record): **Additive engine capability — the
> matching half of the lexical-parity story.** The engine's full-text search is **BM25 only**
> (`FullTextSearchQuery::new(text)`, `manager.rs:1388`/`:1408`): a query token
> matches only stored terms it equals exactly. It has no typo tolerance. Layer
> fronts this engine with a Turbopuffer-shaped wire (`../layer/docs/rfcs/0086-…`,
> the edge twin), and Layer's headline lexical features — `HybridText`, the
> surfacing fallback, and the `hybrid_text`/`fused` routes of `Auto` — are built on
> **per-token fuzzy (edit-distance) matching**. With no engine support they return
> `422 UnsupportedByStore` on a `search` namespace. This is the headline of the
> `label` and `hybrid-text-fusion-demo` workloads, so it is the most user-visible
> engine gap. Fuzzy matching is **engine** work (it is search quality, the moat —
> `CLAUDE.md`). Hard fork: lands here, stays here, no upstream PR.
>
> This RFC stacks on **engine RFC 0001 (alyze tokenizer)**: fuzzy matching is over
> *tokens*, so it must agree with whatever tokenizer indexes the text. Read 0001
> first.

## Summary

Give the engine a **typo-tolerant FTS match primitive**: a query token matches
stored terms within a bounded edit distance, scored into the same BM25 ranking.
Crucially, fuzziness becomes a **property of one FTS query**, not a fan-out of
separate legs — so a single `/query` (FTS or hybrid) is typo-tolerant, and the
engine, not the gateway, does both the matching and the fusion. That is what keeps
Layer's `HybridText` on the right side of the engine/edge split: Layer passes a
fuzziness parameter through; the engine matches and ranks.

## Background: FTS is exact-match BM25 today

- **Query** — `manager.rs:1388` (hybrid leg) and `:1408` (FTS-only) both call
  `FullTextSearchQuery::new(text)`. No fuzziness, no edit distance, no term
  expansion. A token either equals a stored term (post-analysis) or contributes
  nothing.
- **Index** — `manager.rs:1518`, `FtsIndexBuilder::default()` (an inverted BM25
  index; RFC 0001 covers the analyzer). The posting lists are keyed by exact term.
- **Documented surface** — `docs/api.html` query-modes table lists FTS as "BM25
  full-text search"; there is no fuzzy mode.

So `"kubernets"` retrieves nothing against a corpus that only contains
`"kubernetes"`. Turbopuffer's wire, by contrast, supports edit-distance matching,
and Layer's `HybridText` leans on it: it expands a query into a BM25 leg plus one
**fuzzy leg per token** and RRF-fuses them
(`../layer/site/src/content/docs/api/query.mdx` § "Hybrid text fusion").

## Where fusion lives — and why the engine must own the match

Today Layer's `HybridText` fans out N legs (BM25 + one fuzzy per token) and relies
on the **upstream** store to fuse them (Turbopuffer multi-query + `rerank_by`
RRF). hev search has no multi-leg fusion verb, and the engine/edge "one rule"
(`CLAUDE.md`) forbids the gateway from fusing legs itself. The clean resolution is
**not** to teach the gateway to fuse, nor to add an N-leg fusion verb to the
engine — it is to make fuzziness a property of the single FTS match:

- A fuzzy-enabled FTS query already expands each token to its edit-distance
  neighborhood *inside the BM25 query*, so one FTS query is typo-tolerant without N
  separate legs.
- The engine's existing single-call RRF hybrid (vector + FTS, `manager.rs:1388`)
  then fuses semantic + (fuzzy) lexical in one call — fusion stays in the engine.

So Layer's `HybridText`/`Auto` over a `search` namespace collapse to **one**
fuzzy-FTS (or fuzzy-FTS + vector hybrid) query. The gateway supplies the
fuzziness knob and the query string; the engine matches, scores, and fuses. No
gateway-side fusion, no N-leg multi-query — the "one rule" holds.

## Design

### The match primitive

Add an optional fuzziness control to the FTS path:

```
POST /ns/{ns}/query
{ "text": "conection timout kubernets", "k": 10,
  "fuzzy": { "max_edit_distance": "auto" } }     # or 0 | 1 | 2
```

- `"auto"` mirrors Layer's `fuzziness: "auto"` ladder (exact for short tokens,
  distance 1 for medium, distance 2 for long), so the wire spelling maps straight
  through. Fixed `0`/`1`/`2` cap the ladder; `0` is exact-only (today's behavior,
  the default when `fuzzy` is omitted — fully backward compatible).
- Fuzziness applies per analyzed token (it composes with the RFC 0001 analyzer:
  the token stream is fuzzed, the original text is not). It is valid on the
  FTS-only and hybrid modes; combining it with the vector-only mode is a no-op.

### Two implementation paths (pick at the pin)

The engine pins `lancedb = "=0.29.0"`, `lance = "=6.0.0"` (`Cargo.toml:24-25`), and
currently uses only `FullTextSearchQuery::new` — the simplest constructor.

- **Option A — native LanceDB FTS fuzziness (recommended *if* the pin exposes
  it).** lance's inverted index supports structured full-text queries beyond the
  bare string constructor (match queries with parameters). If lance 6.0.0 exposes a
  per-term fuzziness / max-edit-distance on its FTS query type, wire `fuzzy`
  through to it and let lance do the term-dictionary walk. This is the RFC 0001
  posture applied again: prefer the path that **stays on the exact pin** and adds
  no transitive fork. The implementation PR's first task is to confirm whether the
  `=6.0.0` FTS query API carries fuzziness; if yes, Option A is a thin wiring
  change.
- **Option B — engine-side term expansion (fallback if A is unavailable on the
  pin).** At query time, expand each analyzed token to its edit-distance
  neighborhood against the FTS index's term dictionary, then issue a single BM25
  query over the OR of the expansions (optionally down-weighting expanded terms).
  This keeps tokenization (RFC 0001) and matching in the engine and needs read
  access to the index vocabulary; it is more code than A and must bound the
  expansion to keep query latency sane. It does **not** fork a transitive dep,
  which is the deciding constraint over patching lance-index (cf. RFC 0001
  Option B, rejected for exactly that reason).

Lead with A; fall back to B only if the pinned lance FTS API has no fuzziness hook.
Both keep the engine as the single owner of analysis + matching + fusion.

### Scoring

Fuzzy hits score on the same BM25 scale as exact hits so they fuse cleanly in the
hybrid RRF. Whether an edit-distance penalty is applied (so an exact match
outranks a distance-2 match for the same term) is an open question for the eval —
Turbopuffer parity is the target.

## Surfacing fallback (Layer's empty-result path)

Layer's `HybridText` has a "surfacing fallback": when the BM25-over-full-input leg
scores zero (a fully misspelled query), it re-runs fuzzy-only and reorders by edit
distance (`query.mdx` § "Surfacing fallback"). With a fuzzy match primitive in the
engine, this becomes a property of the single query rather than a Layer-side
re-issue: a fuzzy-enabled FTS query already returns near matches for a
fully-misspelled input. Layer can keep its `surfaced` echo semantics; the engine
just stops returning nothing. The exact division (does Layer still detect "BM25
contributed zero" and surface a flag, or does the engine report it) is an edge
concern for RFC 0086, not this RFC — the engine's job is to *match*.

## Open questions (for the implementation PR)

- **Does the `=6.0.0` lance FTS query API expose fuzziness?** Decides A vs B. This
  is the first thing to check.
- **Interaction with alyze (RFC 0001).** Fuzzy must operate on the same token
  surface the index is built over (`text_tok`), with the same analyzer on both
  paths, or recall desyncs. Sequence relative to 0001.
- **The `auto` ladder.** Confirm the exact token-length → distance thresholds
  against Turbopuffer's so the wire maps faithfully (Layer's `query.mdx` documents
  3–5 ⇒ exact, 6–8 ⇒ 1, 9+ ⇒ 2).
- **Score penalty for edit distance** — parity-driven; gate on the eval.
- **Latency / expansion bound** (Option B only) — cap neighborhood size per token.
- **Index format impact.** Option A likely needs no new on-disk format; Option B
  needs the term dictionary readable at query time. Note whether either requires a
  reindex.

## Testing & evaluation

- **Integration** (`crates/hevsearch-api/tests/api_fts.rs`, the FTS modes): the
  exact-match cases stay green with `fuzzy` omitted (default `0`); `"kubernets"`
  with `fuzzy:auto` retrieves the `"kubernetes"` doc; `fuzzy:0` does not; hybrid
  (vector + fuzzy FTS) fuses correctly in one call.
- **Quality.** Reuse the `bench/` BEIR/FiQA harness (as RFC 0001 does) to confirm
  fuzzy matching helps typo-laden queries without regressing clean ones; gate any
  default change on the number.
- **Turbopuffer parity.** Where feasible, assert fuzzy behavior matches the
  upstream wire on a shared corpus — the property the whole change is for.

## Alternatives considered

- **Teach the gateway to fuse N fuzzy legs.** Rejected: violates the one rule
  (`CLAUDE.md`) — fusion/relevance is engine work, and it would reimplement in
  Layer exactly what the engine should own.
- **Add an N-leg multi-query + RRF verb to the engine.** Rejected for fuzzy:
  unnecessary once fuzziness is a property of one FTS match. (A general multi-leg
  verb is a separate question; it is *not* needed to serve `HybridText`.)
- **Trigram / n-gram scalar index for substring/typo.** lance has an `ngram`
  tokenizer and label-list indexes; a trigram index supports fuzzy *contains* but
  on a different scoring model than BM25 and would not fuse cleanly into the hybrid
  RRF. A possible complement, not the primary path.
- **Do nothing.** Leaves `HybridText`/`Auto` permanently `422` on `search`, which
  guts the lexical demos on the owned engine. Rejected as the baseline this RFC
  moves off.

## Fork delta

Pure **additive engine capability** on a hard fork (`AGENTS.md` § "This is a hard
fork") — no upstream PR. The motivation is Layer-opinionated (fuzzy parity with the
Turbopuffer wire Layer fronts the engine with), so like RFC 0001 it is ours, not a
generic upstream contribution. Record the deltas (the `fuzzy` query field, the
chosen match path) so a hand cherry-pick doesn't fight them. No subtractive edge
removal.

## References

- `crates/hevsearch-core/src/manager.rs:1388`/`:1408` — `FullTextSearchQuery::new`
  (exact-match BM25 query, the surface to extend); `:1518` — `FtsIndexBuilder::default()`
  (the BM25 index); `:1391`/`:1411` — `only_if` (filters compose with fuzzy FTS).
- `Cargo.toml:24-25` — `lancedb = "=0.29.0"`, `lance = "=6.0.0"` (the pin Option A
  must check for a fuzziness hook).
- engine RFC 0001 — alyze tokenizer; fuzzy matches tokens, so it must agree with
  the analyzer. Sequence after / alongside.
- `../layer/site/src/content/docs/api/query.mdx` — `HybridText`, the surfacing
  fallback, `Auto` routing, and the `fuzziness` ladder this maps to.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — the edge twin; the
  `422 UnsupportedByStore` the engine returns until this lands.
- `bench/` — BEIR/FiQA quality harness for the eval gate.
- `CLAUDE.md` § "Engine (keep) vs edge (shed)" / "The one rule", `AGENTS.md`
  § "The engine/edge test".
