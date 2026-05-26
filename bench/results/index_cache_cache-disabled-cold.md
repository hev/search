# Index cache bench: cache-disabled-cold

- **Date**: 2026-05-25
- **Mode**: `cache-disabled-cold`
- **Backend**: see `FIRNFLOW_STORAGE_URI` / `FIRNFLOW_S3_ENDPOINT`
- **Namespace**: `index-cache-bench-real-001` (re-used across modes; seed is amortised)
- **Workload**: dim=1536, rows=1000000 (~5859 MB raw), queries=1000 (+100 warmup), k=10, nprobes=20
- **Seed**: skipped (namespace reused)
- **Index cache (mode=warm-index-cache only)**: RAM=1024MB, NVMe=10240MB
- **Result cache**: RAM=16MB, NVMe=256MB

## Latency (1000-query window)

| p50 | p95 | p99 | max |
| ---: | ---: | ---: | ---: |
| 10.16 ms | 19.67 ms | 33.72 ms | 50.33 ms |

## Firn-level S3 requests (delta)

| counter | delta |
| --- | ---: |
| `firnflow_s3_requests_total` for this namespace | 0 |

Caveat: this counter is recorded at Firn's service boundary, not at Lance's internal object-store reads. It tracks how many queries were issued, not how many S3 GETs they triggered. A foyer-warmed index cache reduces Lance's internal reads without changing this number. The right S3-reduction signal for the warm-index-cache run is the cache-backend `insert` delta above (a low `insert` count on a 1000-query warm window implies Lance found most of what it needed without re-fetching it from S3).

## Notes

- The same 1000 measure-window query vectors are used across modes (stable seed). Each is freshly generated and disjoint from the seeded corpus, so every query is novel and the service-level result cache never hits.
- The warmup pass (100 queries on the warm-index-cache mode, zero on the other modes) runs against the manager directly to populate the foyer index cache before the measure window. Other modes deliberately skip the warmup so the baseline does not warm Lance's default in-memory session cache, which would contaminate the comparison.
- Cache-backend invocation counters are aggregate across Lance type names and key prefixes. A per-(type_name, prefix) breakdown is a follow-up; the aggregate is sufficient to read the warm-vs-cold signal.
