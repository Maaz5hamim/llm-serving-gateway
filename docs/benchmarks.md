# Benchmarks

## Scenario

Three e-commerce support tenants send realistic customer support questions through the gateway in three phases. All requests target a single `support` model alias backed by OpenAI `gpt-4o-mini`. No mocks or synthetic providers were used.

**Tenants:**

| Tenant | Tier | RPM limit | Daily budget |
|---|---|---|---|
| ShopEasy | Free | 30 | $0.50 |
| MegaMart | Paid | 120 | $5.00 |
| LuxeBrand | Enterprise | 300 | $20.00 |

**Question pool:** 10 common e-commerce support questions (return policy, order tracking, shipping times, payment methods, etc.). Repeated across tenants and phases to exercise the cache.

**Phases:**

| Phase | Duration | Rate | Purpose |
|---|---|---|---|
| Cold start | 30 s | 1 req/s per tenant | Establishes baseline with all cache misses |
| Cache warm-up | 90 s | 3 req/s per tenant | Repeated questions populate exact-match cache |
| Budget pressure | 30 s | 6 req/s per tenant | High rate stresses rate limiting and budget enforcement |

## Test environment

- **Hardware:** Apple M3 Mac, macOS, Docker Desktop
- **Gateway, Redis, PostgreSQL, Prometheus:** all in Docker Compose on the same host
- **Provider:** OpenAI `gpt-4o-mini` over the public API (real network, real tokens)
- **Client:** `scripts/demo_load.py` — multithreaded Python, one thread per in-flight request

## Results

Results from 4 consecutive runs (each run = all 3 phases):

| Metric | Value |
|---|---|
| Total requests | 5,643 |
| Successful | 3,863 |
| Rate limited | 1,780 |
| Budget blocked | 0 |
| Client errors | 0 |
| Cache hit rate | ~97% at steady state |
| p50 latency | 5 ms |
| p99 latency | 828 ms |
| Cost (actual provider spend) | $0.0039 |
| Cost avoided by cache | $0.1003 |
| **Cost savings** | **96.9%** |

### Latency breakdown

The p99 latency chart shows two regimes:

- **Cold start (~2,500 ms p99):** every request is a cache miss; latency reflects OpenAI network round-trip + model generation time.
- **Warm cache (~5 ms p50, ~800 ms p99):** the majority of requests are sub-millisecond exact-match cache hits. The p99 stays elevated because a small fraction of requests (new questions or cache misses) still go to OpenAI.

Gateway-only overhead (measured via `X-Gateway-Overhead-Ms` header, excluding provider time) is consistently under 3 ms at all tested rates.

### Cost savings

The gateway avoided $0.10 in provider spend on $0.004 of actual spend — a **96.9% cost reduction** — by serving cached responses to repeated questions. The exact-match cache served the same 10-question pool back from Redis at sub-millisecond latency once warmed.

### Rate limiting

1,780 requests were rejected with 429 (rate limited) during the high-rate Phase 3. No requests were dropped silently — all rejections were returned to the client immediately. Zero client errors (non-429/402) were observed across all runs.

## Reliability (failover and chaos)

Tested with Toxiproxy injecting failures into the primary provider connection:

| Scenario | Requests | Client errors | Notes |
|---|---|---|---|
| Primary at 50% error rate | 450 | 0 | All served via fallback |
| Primary offline 60+ s | 2,870 | 0 | All served by backup; first fallback in 251 ms |
| Primary restored | — | 0 | Traffic returned during the 35-second window after recovery |
| Primary 5 s delay | 400 | 0 | Worst-case 1.785 s; routed to backup under 1.5 s timeout |
| Redis restart mid-load | — | 0 | Cache misses during outage; recovered after restart |
| PostgreSQL pause | — | 0 | Keys cached in Redis; requests continued during outage |

Circuit breaker opened after 5 consecutive timeouts and allowed one probe after 30 seconds. No client errors were observed in any chaos scenario.

## Reproducing

```sh
# Run the three-phase demo scenario
python3 scripts/demo_load.py --admin-key $ADMIN_API_KEY

# Run the chaos/reliability tests (requires Toxiproxy in the stack)
python3 scripts/reliability.py
python3 scripts/storage_outages.py
```

Set `ADMIN_API_KEY` before running. The scripts create and revoke their own application keys automatically.
