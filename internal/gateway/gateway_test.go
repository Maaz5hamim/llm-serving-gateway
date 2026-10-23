package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanonical(t *testing.T) {
	a, _, err := canonical([]byte(`{"model":"x", "temperature":0}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _, _ := canonical([]byte(`{ "temperature": 0,"model":"x"}`))
	if string(a) != string(b) {
		t.Fatal("equivalent JSON differs")
	}
	if _, _, err := canonical([]byte(`{} {}`)); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}
func TestCacheAndCoalescing(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		io.WriteString(w, `{"choices":[],"usage":{"total_tokens":16}}`)
	}))
	defer up.Close()
	g := New(Config{APIKey: "secret", Model: "mock-fast", ProviderURL: up.URL, RPM: 1000})
	srv := httptest.NewServer(g.Handler())
	defer srv.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("POST", srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"mock-fast","messages":[{"role":"user","content":"hello"}],"temperature":0}`))
			req.Header.Set("Authorization", "Bearer secret")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer res.Body.Close()
			if res.StatusCode != 200 {
				t.Errorf("status %d", res.StatusCode)
			}
			var body any
			if json.NewDecoder(res.Body).Decode(&body) != nil {
				t.Error("invalid response")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("got %d upstream calls", calls.Load())
	}
}
func TestAuthAndRateLimit(t *testing.T) {
	g := New(Config{APIKey: "secret", Model: "mock-fast", RPM: 1})
	h := g.Handler()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	r.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 429 {
		t.Fatal(w.Code)
	}
}
func TestStreaming(t *testing.T) {
	payload := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, payload)
	}))
	defer up.Close()
	g := New(Config{APIKey: "secret", Model: "mock-fast", ProviderURL: up.URL})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"mock-fast","messages":[{"role":"user","content":"hello"}],"stream":true}`))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, r)
	if w.Code != 200 || w.Body.String() != payload {
		t.Fatalf("unexpected stream: %d %s", w.Code, w.Body.String())
	}
}

func TestStreamCacheReplay(t *testing.T) {
	calls := 0
	payload := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, payload)
	}))
	defer up.Close()
	g := New(Config{APIKey: "secret", Model: "mock-fast", ProviderURL: up.URL})
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"mock-fast","messages":[{"role":"user","content":"hello"}],"temperature":0,"stream":true}`))
		r.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		g.Handler().ServeHTTP(w, r)
		if w.Body.String() != payload {
			t.Fatal(w.Body.String())
		}
		if i == 1 && w.Header().Get("X-Cache") != "hit" {
			t.Fatal("stream was not cached")
		}
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}
