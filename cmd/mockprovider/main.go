package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	var calls atomic.Int64
	var latencyValue atomic.Int64
	var errorValue atomic.Int64
	latency, _ := strconv.Atoi(os.Getenv("MOCK_LATENCY_MS"))
	rate, _ := strconv.ParseFloat(os.Getenv("MOCK_ERROR_RATE"), 64)
	latencyValue.Store(int64(latency))
	errorValue.Store(int64(rate * 1000000))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Latency int64   `json:"latency_ms"`
			Rate    float64 `json:"error_rate"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&c) != nil || c.Latency < 0 || c.Latency > 60000 || c.Rate < 0 || c.Rate > 1 {
			http.Error(w, "invalid config", 400)
			return
		}
		latencyValue.Store(c.Latency)
		errorValue.Store(int64(c.Rate * 1000000))
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"calls": calls.Load()})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req) != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(latencyValue.Load()) * time.Millisecond):
		}
		if rand.Float64() < float64(errorValue.Load())/1000000 {
			http.Error(w, "simulated failure", 503)
			return
		}
		id := fmt.Sprintf("chatcmpl-mock-%d", calls.Load())
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, delta := range []map[string]string{{"role": "assistant"}, {"content": "Hello from the mock provider."}} {
				b, _ := json.Marshal(map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": req.Model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}}})
				fmt.Fprintf(w, "data: %s\n\n", b)
				http.NewResponseController(w).Flush()
			}
			fmt.Fprintf(w, "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":6,\"total_tokens\":16},\"choices\":[]}\n\ndata: {\"id\":%q,\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", id)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": req.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": "Hello from the mock provider."}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16}})
	})
	log.Fatal(http.ListenAndServe(":8081", mux))
}
