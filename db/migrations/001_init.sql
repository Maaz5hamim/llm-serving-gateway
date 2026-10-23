CREATE TABLE IF NOT EXISTS api_keys (
 id UUID PRIMARY KEY, key_hash TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
 rpm_limit INT NOT NULL DEFAULT 60 CHECK(rpm_limit>0),
 daily_tokens BIGINT NOT NULL DEFAULT 1000000 CHECK(daily_tokens>0),
 daily_cost_micros BIGINT NOT NULL DEFAULT 10000000 CHECK(daily_cost_micros>0),
 cache_ttl_sec INT NOT NULL DEFAULT 86400 CHECK(cache_ttl_sec>0),
 revoked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS model_prices(provider TEXT NOT NULL,model TEXT NOT NULL,input_per_mtok NUMERIC NOT NULL,output_per_mtok NUMERIC NOT NULL,PRIMARY KEY(provider,model));
CREATE TABLE IF NOT EXISTS usage (
 id BIGSERIAL PRIMARY KEY, request_id UUID NOT NULL, api_key_id UUID REFERENCES api_keys(id),
 model_alias TEXT NOT NULL, provider TEXT, cache_status TEXT NOT NULL,
 input_tokens INT NOT NULL DEFAULT 0, output_tokens INT NOT NULL DEFAULT 0,
 cost_usd NUMERIC(16,8) NOT NULL DEFAULT 0, saved_usd NUMERIC(16,8) NOT NULL DEFAULT 0,
 provider_ms DOUBLE PRECISION NOT NULL DEFAULT 0, overhead_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
 status INT NOT NULL, attempts INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS usage_key_time ON usage(api_key_id,created_at);
