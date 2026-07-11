# hev search — agent & engineering guide

Engineering rules for the hev search engine. For the strategy and the engine/edge
frame, read `CLAUDE.md`. For the Layer-side direction, read
`../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md`.

## This is a hard fork

- `origin` = `github.com/hev/search`. It began as a fork of
  `github.com/gordonmurray/firnflow` but **no longer tracks it**.
- **We send nothing upstream** and **don't rebase on upstream releases** as a
  discipline. Cherry-pick a specific upstream fix by hand if it's worth having;
  otherwise treat the engine as ours and diverge.
- The original copyright + Apache-2.0 license are retained in `LICENSE`; the engine
  source stays public. Hard fork is a development posture, not a relicensing.

## The engine/edge test (apply before adding anything)

Before adding a feature, ask: **is this engine or edge?** (`CLAUDE.md` defines
the split.)

- **Engine** (search, storage, indexing, caching, metadata, filter, facets) →
  build it here.
- **Edge** (auth, rate limiting, proxy-header trust, anything tenant / authz) →
  **it belongs in Layer, not here. Don't add it.** Layer is the edge; this engine
  runs behind a `NetworkPolicy`.

## Change protocol — additive vs subtractive

Both kinds of change are **local and permanent** — there is no upstream fate to
weigh anymore:

- **Additive engine capability:** land it in the fork. (Filter, metadata columns,
  facets are ours; they are not contributed back.)
- **Subtractive edge removal:** land it in the fork. Record it so a manual upstream
  cherry-pick doesn't silently re-introduce it. Auth is the canonical removal.
- **Never silently re-add a deliberately-removed edge feature** when pulling any
  upstream fix. If a cherry-pick drags auth / rate-limiting back in, that's a
  regression to re-remove, not an update to accept.

## Don't reimplement what Layer owns

Auth, per-tenant authorization, query history, embedding, and the inbound wire
live in `../layer`. If you catch yourself adding one of them here, stop — it's a
boundary error (`CLAUDE.md` § "What the engine is NOT").

## Build & test

The toolchain is containerized; no local Rust needed (see `README.md`):

- Run the suite (needs MinIO): `docker compose up -d minio minio-init` then
  `./scripts/cargo test --workspace -- --ignored`.
- CI runs `cargo test --workspace -- --ignored --skip _aws --skip _100_runs_`.
- Integration tests are `#[tokio::test] #[ignore]` (MinIO-gated), driven through
  the axum router with tower `oneshot`; pure validators are unit-tested inline.
  Match that shape.
- **Images go to the mesh-account ECR via `depot`, never `ghcr.io`.**
  `docker-publish.yml` still targets `ghcr.io/hev/search` (inherited from firnflow)
  — that's a defect to retarget at ECR, not the house style.

## Where findings go

An engine gap or bug is the engine's **own** paper trail now — a GitHub issue or
an RFC on **`hev/search`** (RFCs in `docs/rfcs/`), with the workload as the
motivating case. It does **not** go to `hev/layer` (that's for edge findings) and
it does **not** go upstream to `gordonmurray/firnflow` (hard fork). File on
**GitHub, never Linear**.

## Pointers

- `CLAUDE.md` — the engine/edge strategy and the fork's purpose.
- `../layer/docs/rfcs/0086-hev-search-vectorstore-backend.md` — Layer-side direction.
- `docs/rfcs/` — the engine's own RFCs.
- `README.md` — public product framing, full build/test, storage-backend matrix.
