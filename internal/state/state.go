package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Key holds the attributes of an API key returned from storage.
type Key struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	RPM           int        `json:"rpm_limit"`
	DailyTokens   int64      `json:"daily_tokens"`
	DailyMicros   int64      `json:"daily_cost_micros"`
	TTL           int        `json:"cache_ttl_sec"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	AllowedModels []string   `json:"allowed_models,omitempty"`
	WebhookURL    string     `json:"webhook_url,omitempty"`
}

type Store struct {
	Redis *redis.Client
	DB    *pgxpool.Pool
}

func Hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func Open(ctx context.Context, redisURL, dbURL string) (*Store, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	opt.DialTimeout = time.Second
	opt.ReadTimeout = time.Second
	opt.WriteTimeout = time.Second
	opt.MaxRetries = -1
	opt.ContextTimeoutEnabled = true
	s := &Store{Redis: redis.NewClient(opt)}
	s.DB, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		s.Redis.Close()
		return nil, err
	}
	if err = s.Redis.Ping(ctx).Err(); err == nil {
		err = s.DB.Ping(ctx)
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() { s.Redis.Close(); s.DB.Close() }

// Lookup resolves a raw bearer key to its Key record. PostgreSQL results are
// cached in Redis for 30 s to tolerate brief database outages. Expired and
// revoked keys return an error.
func (s *Store) Lookup(ctx context.Context, raw string) (Key, error) {
	var k Key
	if s.Get(ctx, "auth:"+Hash(raw), &k) {
		return k, nil
	}
	var webhookURL *string
	err := s.DB.QueryRow(ctx,
		`SELECT id::text,name,rpm_limit,daily_tokens,daily_cost_micros,cache_ttl_sec,expires_at,allowed_models,webhook_url
		 FROM api_keys
		 WHERE key_hash=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())`,
		Hash(raw)).Scan(&k.ID, &k.Name, &k.RPM, &k.DailyTokens, &k.DailyMicros, &k.TTL,
		&k.ExpiresAt, &k.AllowedModels, &webhookURL)
	if err != nil {
		return k, err
	}
	if webhookURL != nil {
		k.WebhookURL = *webhookURL
	}
	if k.AllowedModels == nil {
		k.AllowedModels = []string{}
	}
	s.Put(ctx, "auth:"+Hash(raw), k, 30*time.Second)
	return k, nil
}

// Create inserts a new API key and returns the stored record and the raw key
// value (returned only once, never stored).
func (s *Store) Create(ctx context.Context, k Key) (Key, string, error) {
	raw := "gw_" + ID() + ID()
	k.ID = ID()
	if k.AllowedModels == nil {
		k.AllowedModels = []string{}
	}
	var webhookURL interface{}
	if k.WebhookURL != "" {
		webhookURL = k.WebhookURL
	}
	_, err := s.DB.Exec(ctx,
		`INSERT INTO api_keys(id,key_hash,name,rpm_limit,daily_tokens,daily_cost_micros,cache_ttl_sec,expires_at,allowed_models,webhook_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		k.ID, Hash(raw), k.Name, k.RPM, k.DailyTokens, k.DailyMicros, k.TTL, k.ExpiresAt, k.AllowedModels, webhookURL)
	return k, raw, err
}

func (s *Store) Revoke(ctx context.Context, id string) error {
	var hash string
	err := s.DB.QueryRow(ctx,
		`UPDATE api_keys SET revoked_at=now() WHERE id::text=$1 AND revoked_at IS NULL RETURNING key_hash`, id).Scan(&hash)
	if err != nil {
		return err
	}
	if e := s.Redis.Del(ctx, "auth:"+hash).Err(); e != nil {
		return errors.New("key revoked in database; authentication cache may persist for 30 seconds")
	}
	return nil
}

// SendWebhook fires a JSON POST to url asynchronously. Errors are silently
// dropped since webhooks are best-effort notifications.
func (s *Store) SendWebhook(url string, payload map[string]any) {
	go func() {
		b, err := json.Marshal(payload)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
}

var rateScript = redis.NewScript(`local b=redis.call('HMGET',KEYS[1],'tokens','ts');local t=tonumber(b[1]) or tonumber(ARGV[1]);local ts=tonumber(b[2]) or tonumber(ARGV[2]);local now=tonumber(ARGV[2]);t=math.min(tonumber(ARGV[1]),t+math.max(0,now-ts)*tonumber(ARGV[1])/60000);local ok=0;if t>=1 then t=t-1;ok=1 end;redis.call('HSET',KEYS[1],'tokens',t,'ts',now);redis.call('PEXPIRE',KEYS[1],120000);return {ok,math.floor(t),math.ceil((1-t)*60000/tonumber(ARGV[1]))}`)

func (s *Store) Limit(ctx context.Context, k Key) (bool, int, int, error) {
	v, err := rateScript.Run(ctx, s.Redis, []string{"rate:" + k.ID}, k.RPM, time.Now().UnixMilli()).Int64Slice()
	if err != nil {
		return false, 0, 0, err
	}
	return v[0] == 1, int(v[1]), max(1, int((v[2]+999)/1000)), nil
}

var reserveScript = redis.NewScript(`local t=tonumber(redis.call('HGET',KEYS[1],'tokens') or '0');local c=tonumber(redis.call('HGET',KEYS[1],'cost') or '0');local nt=t+tonumber(ARGV[1]);local nc=c+tonumber(ARGV[2]);if nt>tonumber(ARGV[3]) or nc>tonumber(ARGV[4]) then return {0,math.max(0,tonumber(ARGV[3])-t)} end;redis.call('HSET',KEYS[1],'tokens',nt,'cost',nc);redis.call('EXPIRE',KEYS[1],172800);return {1,math.max(0,tonumber(ARGV[3])-nt)}`)

func BudgetKey(id string) string { return "budget:" + id + ":" + time.Now().UTC().Format("2006-01-02") }
func (s *Store) Reserve(ctx context.Context, k Key, bkey string, tokens, cost int64) (bool, int64, error) {
	v, e := reserveScript.Run(ctx, s.Redis, []string{bkey}, tokens, cost, k.DailyTokens, k.DailyMicros).Int64Slice()
	if e != nil {
		return false, 0, e
	}
	return v[0] == 1, v[1], nil
}

var settleScript = redis.NewScript(`redis.call('HINCRBY',KEYS[1],'tokens',ARGV[1]);redis.call('HINCRBY',KEYS[1],'cost',ARGV[2]);redis.call('EXPIRE',KEYS[1],172800);return 1`)

func (s *Store) Settle(ctx context.Context, bkey string, tokens, cost int64) error {
	return settleScript.Run(ctx, s.Redis, []string{bkey}, tokens, cost).Err()
}
func (s *Store) Get(ctx context.Context, key string, dst any) bool {
	b, e := s.Redis.Get(ctx, key).Bytes()
	return e == nil && json.Unmarshal(b, dst) == nil
}
func (s *Store) Put(ctx context.Context, key string, value any, ttl time.Duration) {
	b, e := json.Marshal(value)
	if e == nil {
		s.Redis.Set(ctx, key, b, ttl)
	}
}
