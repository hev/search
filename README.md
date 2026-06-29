# hev search

**hev search** is a vector, full-text, and hybrid search engine that runs directly on object storage. It pairs LanceDB (vector + BM25 search over S3) with foyer (a RAM + NVMe cache), so data sits on cheap object storage while repeated queries are served from cache without a backend round-trip.

It is a hard fork of [firnflow](https://github.com/gordonmurray/firnflow) by Gordon Murray, now developed independently. The original copyright and [Apache-2.0 license](LICENSE) are retained.

## Built to run behind hev layer

hev search is the **engine**. It is designed to run behind **[hev layer](https://hevlayer.com)**, the gateway that fronts it — Layer is its only client, reachable over a `NetworkPolicy`. This split is the main difference from stock firnflow:

- **Layer owns the edge** — authentication, per-tenant authorization, rate limiting, the inbound (Turbopuffer-shaped) API, and embedding. **Multi-tenancy lives at the gateway**: Layer's scoped keys bind a caller to its namespace(s).
- **hev search owns the engine** — vector / FTS / hybrid search, LanceDB storage on object storage, the index lifecycle, and the caches. The only tenancy concept it keeps is physical: a namespace is an isolated object-storage prefix.

The engine itself is an open, trusted internal service — it does no auth of its own. Don't expose it directly; put Layer (or another authenticating gateway) in front.

## Performance

Benchmarked at 100,000 vectors of 1536 dimensions against AWS S3 in `eu-west-1`:

| Query path | p50 latency |
| --- | --- |
| Cold, no index (brute-force scan over S3) | ~25.1 s |
| Cold, IVF_PQ index (first run of a query) | ~979 ms |
| Warm (byte-identical repeat, served from cache) | ~72 µs |
| End-to-end HTTP, warm | < 5 ms |

Two things decide latency. An **IVF_PQ index** turns an unindexed ~25 s scan into a ~979 ms cold query — build it with `POST /ns/{ns}/index` after your first writes. The **result cache** returns byte-identical repeats in microseconds; any write advances the namespace version and drops its cached results, so it never serves stale data. Novel queries miss the result cache and pay the cold cost; an optional NVMe **object cache** (`HEVSEARCH_OBJECT_CACHE_ENABLED=true`) keeps the underlying S3 byte-ranges local so even new queries over already-read data skip the round-trips.

## Architecture

Tiered storage:

1. **L1 — RAM cache** (foyer): microsecond reads for the hottest queries.
2. **L2 — NVMe cache** (foyer): durable cache for high-volume results.
3. **L3 — object storage** (LanceDB on S3): the source of truth; each namespace is its own object-storage prefix.

Built on **axum** (REST API), **LanceDB** (vector + BM25 on object storage), **foyer** (hybrid RAM/NVMe cache), and **Prometheus** (cache-hit and backend-request metrics).

## Quickstart

The engine speaks a small internal REST API. In production you reach it through hev layer; the calls below talk to it directly for local development.

### 1. Launch the stack

MinIO storage + the hev search API, via Docker Compose:

```bash
git clone https://github.com/hev/search
cd search
docker compose up --build
```

### 2. Upsert a vector

The API is live at `http://localhost:3000`:

```bash
curl -X POST http://localhost:3000/ns/demo/upsert \
     -H 'Content-Type: application/json' \
     -d '{"rows": [{"id": 1, "vector": [1.0, 0.0, 0.0, 0.0], "attributes": {"section": "warnings"}}]}'
```

Upsert is keyed by `id` and latest-write-wins. Rows may carry scalar `attributes` for filtering and facets.

### 3. Search

```bash
curl -X POST http://localhost:3000/ns/demo/query \
     -H 'Content-Type: application/json' \
     -d '{"vector": [1.0, 0.0, 0.0, 0.0], "k": 1}'
```

Add `"filter": "id > 1000"` to scope the search, or `"include_vector": false` to drop vectors from the response.

### 4. Check the savings

```bash
curl http://localhost:3000/metrics | grep s3_requests
```

(`hevsearch_s3_requests_total` counts requests against whichever backend is configured.)

## Storage backend

Backend choice is operator config, not a recompile. Point hev search at a bucket with `HEVSEARCH_STORAGE_URI`. The supported, validated path is **AWS S3** (and S3-compatible MinIO for local dev):

```bash
# AWS S3
HEVSEARCH_STORAGE_URI=s3://my-hevsearch-bucket
HEVSEARCH_S3_REGION=eu-west-1
# Credentials from the standard AWS chain (instance profile, AWS_ACCESS_KEY_ID/…).

# MinIO (local / self-hosted)
HEVSEARCH_STORAGE_URI=s3://hevsearch
HEVSEARCH_S3_ENDPOINT=http://localhost:9000
HEVSEARCH_S3_ACCESS_KEY=minioadmin
HEVSEARCH_S3_SECRET_KEY=minioadmin
```

`HEVSEARCH_STORAGE_URI` takes an optional prefix (`s3://shared-bucket/tenants/acme`) when several deployments share one bucket; namespace tables live at `{root}/{namespace}/`. Correctness depends on the store offering linearizable compare-and-swap (`If-None-Match: *`) for Lance's commit protocol. Other S3-family backends and native GCS have been validated against this contract — see [the docs](https://hevsearch.com/configuration.html) for their config and the full compatibility matrix.

## Python package

hev search also ships as [`hevsearch` on PyPI](https://pypi.org/project/hevsearch/) for embedded use — vector, BM25, and hybrid search in your Python process, no server:

```bash
pip install hevsearch
```

```python
import hevsearch
db = hevsearch.connect("./hevsearch_data")  # local folder, or storage_url="s3://bucket"
db.add([{"id": 1, "vector": [1.0, 0.0, 0.0, 0.0], "text": "the quick brown fox"}])
for hit in db.search("fox", vector=[1.0, 0.0, 0.0, 0.0], limit=3):
    print(hit.id, hit.score, hit.text)
```

Runnable examples live in [`examples/`](examples/).

## Development

Containerized toolchain — no local Rust needed:

```bash
# Full test suite (requires MinIO)
./scripts/cargo test --workspace -- --ignored

# Cold-vs-warm latency benchmark
./scripts/cargo run --release -p hevsearch-bench
```
