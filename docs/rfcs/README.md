# Engine RFCs

Design proposals for **hev search** (the engine behind hev layer; a hard fork of
firnflow developed as ours). These are **engine-scoped** — search, storage,
indexing, caching, metadata, filter, facets, tokenization. The *edge* view (how
Layer operates this engine) lives in `../../../layer/docs/rfcs/` — RFC 0086 there
is the Layer-side twin of this engine.

This series is numbered **independently** of Layer's series; an engine `0001` and
a Layer `0001` are unrelated. Apply the engine/edge test (`AGENTS.md`) before
adding one: if it's edge (auth, tenancy, rate limiting), it belongs in `../layer`,
not here.

| RFC | Title | State |
|-----|-------|-------|
| [0001](0001-alyze-tokenizer.md) | alyze tokenizer for full-text search | Draft. Replace LanceDB's built-in FTS analyzer with [alyze](https://github.com/turbopuffer/alyze) (`word_v4`) run as an external analyzer (pre-tokenize write + query paths; LanceDB reduced to a passthrough whitespace tokenizer + BM25). Motivated by **Turbopuffer wire parity** — Turbopuffer's BM25 is `word_v4`, the dialect Layer translates onto this engine. Engine-local (hard fork; not contributed upstream). |
| [0002](0002-remove-auth.md) | Remove auth, rate limiting, and proxy-header trust | Draft. The flagship subtractive removal: delete the `auth`/`rate_limit` modules, the `HEVSEARCH_API_KEY`/`ADMIN`/`METRICS_TOKEN`/`RATE_LIMIT_*`/`TRUST_PROXY_HEADERS` config, and the `subtle`/`governor`/`tower_governor` deps (~900–1000 lines). Layer is the auth boundary and the engine's only client; the engine is a trusted internal service behind a `NetworkPolicy`. Keeps physical namespace-prefix isolation (engine, not authz). Engine-local + permanent. |
