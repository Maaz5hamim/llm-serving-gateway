package router

import (
	"context"
	"encoding/json"
	"io"
	"llm-serving-gateway/internal/telemetry"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFailoverAndNoRetryOn400(t *testing.T) {
	for _, status := range []int{503, 400} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mu.Lock(); calls++; mu.Unlock(); w.WriteHeader(status) }))
			defer primary.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, `{"usage":{"prompt_tokens":10,"completion_tokens":5},"choices":[]}`)
			}))
			defer backup.Close()
			r := New(Config{Models: map[string][]Provider{"chat": {{Name: "p", Kind: "openai", URL: primary.URL, Model: "p"}, {Name: "b", Kind: "openai", URL: backup.URL, Model: "b", InputPrice: 1, OutputPrice: 2}}}}, telemetry.New(nil))
			v, e := r.Execute(context.Background(), "chat", []byte(`{"messages":[]}`), false, nil)
			if status == 400 {
				if e == nil || calls != 1 {
					t.Fatal("400 retried or succeeded")
				}
			} else if e != nil || v.Provider != "b" || v.Attempts != 3 || v.Cost != 0.00002 {
				t.Fatalf("%+v %v", v, e)
			}
		})
	}
}
func TestBreaker(t *testing.T) {
	r := New(Config{}, telemetry.New(nil))
	for i := 0; i < 20; i++ {
		if !r.allow("p") {
			t.Fatal("opened early")
		}
		r.finish("p", context.DeadlineExceeded)
		if i == 4 {
			break
		}
	}
	if r.allow("p") {
		t.Fatal("five timeouts did not open")
	}
	r.mu.Lock()
	r.breakers["p"].opened = time.Now().Add(-31 * time.Second)
	r.mu.Unlock()
	if !r.allow("p") || r.allow("p") {
		t.Fatal("probe not exclusive")
	}
	r.finish("p", nil)
	if !r.allow("p") {
		t.Fatal("did not recover")
	}
}
func TestAnthropic(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" || r.Header.Get("anthropic-version") == "" {
			t.Error("bad adapter request")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["system"] != "Be concise" {
			t.Error("system lost")
		}
		io.WriteString(w, `{"id":"msg_1","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`)
	}))
	defer up.Close()
	r := New(Config{Models: map[string][]Provider{"chat": {{Name: "a", Kind: "anthropic", URL: up.URL, Model: "model"}}}}, telemetry.New(nil))
	v, e := r.Execute(context.Background(), "chat", []byte(`{"messages":[{"role":"system","content":"Be concise"},{"role":"user","content":"hello"}]}`), false, nil)
	if e != nil || v.Input != 3 || !strings.Contains(string(v.Body), `"content":"hello"`) {
		t.Fatalf("%+v %v", v, e)
	}
}
func TestPartialStreamNeverRetried(t *testing.T) {
	calls := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[]}\n\n")
	}))
	defer up.Close()
	r := New(Config{Models: map[string][]Provider{"chat": {{Name: "p", Kind: "openai", URL: up.URL, Model: "p"}}}}, telemetry.New(nil))
	_, e := r.Execute(context.Background(), "chat", []byte(`{}`), true, func([]byte) error { return nil })
	if e == nil || calls != 1 {
		t.Fatal("partial stream was retried")
	}
}

func TestAnthropicStream(t *testing.T) {
	source := `data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":3}}}

data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}

data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}

data: {"type":"message_stop"}

`
	var out Result
	var frames strings.Builder
	e := readStream(strings.NewReader(source), Provider{Kind: "anthropic"}, "chat", &out, func(b []byte) error { frames.Write(b); return nil })
	if e != nil || out.Input != 3 || out.Output != 1 || !strings.Contains(frames.String(), `"content":"hello"`) || !strings.HasSuffix(frames.String(), "data: [DONE]\n\n") {
		t.Fatal(out, e, frames.String())
	}
}
