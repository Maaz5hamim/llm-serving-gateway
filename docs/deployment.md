# Deployment

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Python 3.9+ (for the demo load script)
- An OpenAI API key with access to `gpt-4o-mini`

## 1. Configure secrets

Create `deploy/.env` (gitignored):

```sh
echo "ADMIN_API_KEY=$(openssl rand -hex 32)" >> deploy/.env
echo "OPENAI_API_KEY=sk-..." >> deploy/.env
```

| Variable | Required | Description |
|---|---|---|
| `ADMIN_API_KEY` | Yes | Master key for `/admin/*` endpoints |
| `OPENAI_API_KEY` | Yes | Forwarded to OpenAI for the `support` model alias |

## 2. Configure model routing

Edit `config/models.yaml`. This file controls which providers handle each model alias and in what order.

### Single provider

```yaml
timeout_ms: 30000

models:
  support:
    - provider: openai
      kind: openai
      url: https://api.openai.com/v1
      key_env: OPENAI_API_KEY
      model: gpt-4o-mini
      input_per_mtok: 0.150
      output_per_mtok: 0.600
```

### Multiple providers with automatic failover

Add more entries under the same alias. The gateway tries them top-to-bottom on errors, timeouts, or rate limits:

```yaml
timeout_ms: 30000

models:
  support:
    - provider: openai
      kind: openai
      url: https://api.openai.com/v1
      key_env: OPENAI_API_KEY
      model: gpt-4o-mini
      input_per_mtok: 0.150
      output_per_mtok: 0.600

    - provider: anthropic
      kind: anthropic
      url: https://api.anthropic.com/v1
      key_env: ANTHROPIC_API_KEY
      model: claude-haiku-4-5
      input_per_mtok: 0.800
      output_per_mtok: 4.000
```

Add the corresponding key to `deploy/.env`:

```sh
echo "ANTHROPIC_API_KEY=sk-ant-..." >> deploy/.env
```

### Multiple model aliases (tiers)

Each alias is independent — clients send the alias in the `model` field:

```yaml
models:
  support:          # cheap, high-volume
    - provider: openai
      kind: openai
      url: https://api.openai.com/v1
      key_env: OPENAI_API_KEY
      model: gpt-4o-mini
      input_per_mtok: 0.150
      output_per_mtok: 0.600

  premium:          # expensive, low-volume
    - provider: openai
      kind: openai
      url: https://api.openai.com/v1
      key_env: OPENAI_API_KEY
      model: gpt-4o
      input_per_mtok: 2.500
      output_per_mtok: 10.000
```

## 3. Start the stack

```sh
cd deploy
docker compose up --build -d
```

| Service | Port | Purpose |
|---|---|---|
| `gateway` | 8080 | OpenAI-compatible API |
| `redis` | — (internal) | Rate limits, budgets, exact-match cache |
| `postgres` | — (internal) | API keys, usage records |
| `prometheus` | — (internal) | Metrics scraper |
| `dashboard` | **8888** | Monitoring dashboard (nginx) |

Wait for the gateway to be healthy:

```sh
curl -s http://localhost:8080/readyz
# → ok
```

## 4. Create API key tiers

Every client application gets its own gateway key with its own limits. The admin key creates them:

```sh
curl -X POST http://localhost:8080/admin/keys \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-app",
    "rpm_limit": 60,
    "daily_cost_micros": 5000000,
    "daily_tokens": 500000,
    "allowed_models": ["support"],
    "expires_at": "2026-12-31T23:59:59Z",
    "webhook_url": "https://example.com/hooks/budget"
  }'
```

| Field | Type | Description |
|---|---|---|
| `name` | string | Human-readable label (required) |
| `rpm_limit` | int | Max requests per minute across all replicas |
| `daily_cost_micros` | int | Daily spend cap in millionths of USD (5 000 000 = $5.00) |
| `daily_tokens` | int | Daily token cap (input + output combined) |
| `allowed_models` | string[] | Model aliases this key may use; empty = all |
| `expires_at` | RFC3339 | Key expiry; omit for no expiry |
| `webhook_url` | string | POST target when budget is exhausted |

The response includes the `key` value (prefix `gw_`). Store it — it is not recoverable after creation.

To revoke a key:

```sh
curl -X DELETE http://localhost:8080/admin/keys/<key-id> \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

## 5. Monitoring dashboard

Open **http://localhost:8888** in your browser. On first visit enter:

- **Admin API key** — your `ADMIN_API_KEY` value
- **Gateway URL** — `/gateway` (pre-filled)
- **Prometheus URL** — `/prometheus` (pre-filled)

Click **Open Dashboard**. The key is stored in your browser's `localStorage` and never sent outside the local network.

## Usage API

Query aggregated usage:

```sh
curl "http://localhost:8080/admin/usage?from=2026-10-01T00:00:00Z&to=2026-10-31T23:59:59Z" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

Query per-tenant breakdown:

```sh
curl "http://localhost:8080/admin/usage?from=2026-10-01T00:00:00Z&to=2026-10-31T23:59:59Z&group_by=key" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

## Smoke test

```sh
# Create a test key
KEY=$(curl -sf -X POST http://localhost:8080/admin/keys \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"smoke","rpm_limit":60}' | python3 -c "import sys,json; print(json.load(sys.stdin)['key'])")

# Send a completion
curl -s http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"support","messages":[{"role":"user","content":"ping"}],"temperature":0,"max_tokens":5}'
```

Check response headers: `X-Cache`, `X-Provider`, `X-Gateway-Overhead-Ms`.

## Local development (no Docker)

```sh
export DATABASE_URL="postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"
export REDIS_URL="redis://localhost:6379"
export ADMIN_API_KEY="dev-admin-key"
export OPENAI_API_KEY="sk-..."
export MODELS_CONFIG="config/models.yaml"

go run ./cmd/gateway
```

Schema migrations run automatically on startup from `db/migrations/`.

## Stopping

```sh
cd deploy && docker compose down          # keep volumes
cd deploy && docker compose down -v       # also remove volumes
```
