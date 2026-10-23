package gateway

import (
	"context"
	"encoding/json"
	"io"
	"llm-serving-gateway/internal/router"
	"llm-serving-gateway/internal/state"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPersistentReplicas(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("integration database not configured")
	}
	ctx := context.Background()
	store, e := state.Open(ctx, os.Getenv("TEST_REDIS_URL"), os.Getenv("TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		io.WriteString(w, `{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":6}}`)
	}))
	defer up.Close()
	var servers []*httptest.Server
	for i := 0; i < 3; i++ {
		g := New(Config{AdminKey: "admin", Store: store, Routes: router.Config{Models: map[string][]router.Provider{"chat": {{Name: "mock", Kind: "openai", URL: up.URL, Model: "mock", InputPrice: 1, OutputPrice: 2}}}}})
		defer g.Close()
		srv := httptest.NewServer(g.Handler())
		defer srv.Close()
		servers = append(servers, srv)
	}
	key, raw, e := store.Create(ctx, state.Key{Name: "replicas", RPM: 1000, DailyTokens: 1000000, DailyMicros: 1000000, TTL: 60})
	if e != nil {
		t.Fatal(e)
	}
	post := func(server int, raw string, payload string) (int, string, []byte) {
		r, _ := http.NewRequest("POST", servers[server].URL+"/v1/chat/completions", strings.NewReader(payload))
		r.Header.Set("Authorization", "Bearer "+raw)
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Error(e)
			return 0, "", nil
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("X-Cache"), b
	}
	payload := `{"model":"chat","messages":[{"role":"user","content":"hi"}],"temperature":0,"max_tokens":20}`
	status, cache, b := post(0, raw, payload)
	if status != 200 || cache != "miss" || !json.Valid(b) {
		t.Fatalf("%d %s %s", status, cache, b)
	}
	status, cache, _ = post(1, raw, payload)
	if status != 200 || cache != "hit" || calls.Load() != 1 {
		t.Fatal("cross replica cache failed", status, cache, calls.Load())
	}
	limited, limitedRaw, e := store.Create(ctx, state.Key{Name: "limit", RPM: 5, DailyTokens: 1000000, DailyMicros: 1000000, TTL: 60})
	if e != nil {
		t.Fatal(e)
	}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, _, _ := post(i%3, limitedRaw, payload)
			if status == 200 {
				ok.Add(1)
			} else if status != 429 {
				t.Errorf("status %d", status)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 5 {
		t.Fatalf("distributed rpm allowed %d", ok.Load())
	}
	small, smallRaw, e := store.Create(ctx, state.Key{Name: "budget", RPM: 60, DailyTokens: 1, DailyMicros: 1, TTL: 60})
	if e != nil {
		t.Fatal(e)
	}
	status, _, _ = post(2, smallRaw, payload)
	if status != 402 {
		t.Fatal("budget", status)
	}
	if e = store.Revoke(ctx, key.ID); e != nil {
		t.Fatal(e)
	}
	status, _, _ = post(1, raw, payload)
	if status != 401 {
		t.Fatal("revoked key", status)
	}
	_ = limited
	_ = small
}

func TestFailureRefund(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("integration database not configured")
	}
	ctx := context.Background()
	store, e := state.Open(ctx, os.Getenv("TEST_REDIS_URL"), os.Getenv("TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	key, raw, e := store.Create(ctx, state.Key{Name: "refund", RPM: 100, DailyTokens: 10000, DailyMicros: 10000, TTL: 60})
	if e != nil {
		t.Fatal(e)
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer up.Close()
	g := New(Config{Store: store, Routes: router.Config{Models: map[string][]router.Provider{"chat": {{Name: "failed", Kind: "openai", URL: up.URL, Model: "mock", InputPrice: 1, OutputPrice: 1}}}}})
	defer g.Close()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"chat","messages":[{"role":"user","content":"hi"}],"max_tokens":20}`))
	r.Header.Set("Authorization", "Bearer "+raw)
	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	v, e := store.Redis.HGetAll(ctx, state.BudgetKey(key.ID)).Result()
	if e != nil || v["tokens"] != "0" || v["cost"] != "0" {
		t.Fatal(v, e)
	}
}
