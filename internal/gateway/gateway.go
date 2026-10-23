package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"llm-serving-gateway/internal/router"
	"llm-serving-gateway/internal/semantic"
	"llm-serving-gateway/internal/state"
	"llm-serving-gateway/internal/telemetry"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type Config struct {
	APIKey, AdminKey, ProviderURL, Model string
	RPM                                  int
	CacheTTL                                          time.Duration
	Store                                             *state.Store
	Routes                                            router.Config
	Semantic                                          *semantic.Config
}
type entry struct {
	Result  router.Result
	Expires time.Time
}
type flight struct {
	done   chan struct{}
	result router.Result
	err    error
}
type bucket struct {
	tokens float64
	at     time.Time
}
type Server struct {
	cfg      Config
	mu       sync.Mutex
	cache    map[string]entry
	flights  map[string]*flight
	bucket   bucket
	router   *router.Router
	meter    *telemetry.Meter
	semantic *semantic.Cache
}

func New(c Config) *Server {
	if c.RPM <= 0 {
		c.RPM = 60
	}
	if c.CacheTTL <= 0 {
		c.CacheTTL = 24 * time.Hour
	}
	if len(c.Routes.Models) == 0 {
		c.Routes = router.Config{Models: map[string][]router.Provider{c.Model: {{Name: "openai-compatible", Kind: "openai", URL: c.ProviderURL, Model: c.Model, KeyEnv: "PROVIDER_API_KEY"}}}}
	}
	var m *telemetry.Meter
	if c.Store != nil {
		m = telemetry.New(c.Store.DB)
	} else {
		m = telemetry.New(nil)
	}
	s := &Server{
		cfg: c, cache: map[string]entry{}, flights: map[string]*flight{},
		bucket: bucket{float64(c.RPM), time.Now()}, router: router.New(c.Routes, m), meter: m,
	}
	if c.Semantic != nil && c.Store != nil {
		s.semantic = semantic.New(*c.Semantic, c.Store.DB)
	}
	return s
}
func (s *Server) Close() { s.meter.Close() }
func failure(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": msg, "type": "gateway_error"}})
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Store != nil {
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if s.cfg.Store.Redis.Ping(ctx).Err() != nil || s.cfg.Store.DB.Ping(ctx) != nil {
				failure(w, 503, "Dependencies unavailable")
				return
			}
		}
		io.WriteString(w, "ok\n")
	})
	mux.Handle("GET /metrics", s.meter.Handler())
	mux.HandleFunc("/v1/", s.authorized)
	mux.HandleFunc("/admin/", s.admin)
	return mux
}
func bearer(r *http.Request) string {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}
func equal(a, b string) bool {
	return a != "" && b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) authorized(w http.ResponseWriter, r *http.Request) {
	// Extract W3C TraceContext from request headers, then start a gateway span.
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("gateway").Start(ctx, "gateway.request",
		oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	defer span.End()
	r = r.WithContext(ctx)

	reqID := state.ID()
	w.Header().Set("X-Request-ID", reqID)

	var k state.Key
	start := time.Now()
	tracked := &statusWriter{ResponseWriter: w}
	w = tracked
	handled := false
	defer func() {
		if !handled {
			status := tracked.status
			if status == 0 {
				status = 200
			}
			key := ""
			if s.cfg.Store != nil {
				key = k.ID
			}
			s.meter.Record(telemetry.Event{ID: reqID, Key: key, Model: "unknown", Cache: "bypass", Status: status, OverheadMS: float64(time.Since(start).Microseconds()) / 1000})
			span.SetAttributes(attribute.Int("http.status_code", status))
			if status >= 500 {
				span.SetStatus(otelcodes.Error, "server error")
			}
		}
	}()

	var err error
	if s.cfg.Store != nil {
		k, err = s.cfg.Store.Lookup(r.Context(), bearer(r))
		if err != nil {
			failure(w, 401, "Invalid or unavailable API key")
			return
		}
	} else {
		if !equal(bearer(r), s.cfg.APIKey) {
			failure(w, 401, "Invalid API key")
			return
		}
		k = state.Key{ID: "local", RPM: s.cfg.RPM, DailyTokens: 1000000000, DailyMicros: 1000000000, TTL: int(s.cfg.CacheTTL.Seconds())}
	}
	span.SetAttributes(attribute.String("gateway.key_id", k.ID))

	if r.URL.Path == "/v1/models" && r.Method == "GET" {
		models := []any{}
		for alias := range s.cfg.Routes.Models {
			models = append(models, map[string]string{"id": alias, "object": "model", "owned_by": "gateway"})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": models})
		return
	}
	if r.URL.Path != "/v1/chat/completions" {
		failure(w, 404, "Unknown endpoint")
		return
	}
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		failure(w, 405, "Method not allowed")
		return
	}
	allowed, remaining, retry := false, 0, 1
	if s.cfg.Store != nil {
		allowed, remaining, retry, err = s.cfg.Store.Limit(r.Context(), k)
		if err != nil {
			w.Header().Set("Retry-After", "1")
			failure(w, 503, "Rate limiter unavailable")
			return
		}
	} else {
		s.mu.Lock()
		now := time.Now()
		s.bucket.tokens = min(float64(k.RPM), s.bucket.tokens+now.Sub(s.bucket.at).Seconds()*float64(k.RPM)/60)
		s.bucket.at = now
		allowed = s.bucket.tokens >= 1
		if allowed {
			s.bucket.tokens--
		}
		remaining = int(s.bucket.tokens)
		retry = (60 + k.RPM - 1) / k.RPM
		s.mu.Unlock()
	}
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	if !allowed {
		s.meter.Limited.Inc()
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		failure(w, 429, "Rate limit exceeded")
		return
	}
	handled = true
	s.chat(w, r, k, start, reqID, span)
}

func canonical(raw []byte) ([]byte, map[string]json.RawMessage, error) {
	var req map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&req); err != nil {
		return nil, nil, err
	}
	if req == nil {
		return nil, nil, fmt.Errorf("Expected object")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, nil, fmt.Errorf("Expected one JSON object")
	}
	var normalized any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&normalized); err != nil {
		return nil, nil, err
	}
	b, err := json.Marshal(normalized)
	return b, req, err
}
func (s *Server) cached(ctx context.Context, key string) (router.Result, bool) {
	var v router.Result
	if s.cfg.Store != nil {
		ok := s.cfg.Store.Get(ctx, key, &v)
		return v, ok
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[key]
	return e.Result, ok && time.Now().Before(e.Expires)
}
func (s *Server) put(ctx context.Context, key string, v router.Result, ttl int) {
	if s.cfg.Store != nil {
		s.cfg.Store.Put(ctx, key, v, time.Duration(ttl)*time.Second)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) >= 10000 {
		s.cache = map[string]entry{}
	}
	s.cache[key] = entry{v, time.Now().Add(time.Duration(ttl) * time.Second)}
}

// waitDistributed polls the cache until a distributed leader (another replica)
// stores the result or the context expires.
func (s *Server) waitDistributed(ctx context.Context, key string) (router.Result, bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(24 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return router.Result{}, false
		case <-deadline.C:
			return router.Result{}, false
		case <-ticker.C:
			if result, ok := s.cached(ctx, key); ok {
				return result, true
			}
		}
	}
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request, k state.Key, start time.Time, reqID string, span oteltrace.Span) {
	event := telemetry.Event{ID: reqID, Model: "unknown", Cache: "bypass", Status: 400}
	if s.cfg.Store != nil {
		event.Key = k.ID
	}
	defer func() {
		event.OverheadMS = max(0, float64(time.Since(start).Microseconds())/1000-event.ProviderMS)
		s.meter.Record(event)
		span.SetAttributes(
			attribute.Int("http.status_code", event.Status),
			attribute.String("gateway.model", event.Model),
			attribute.String("gateway.cache", event.Cache),
			attribute.String("gateway.provider", event.Provider),
		)
		if event.Status >= 500 {
			span.SetStatus(otelcodes.Error, "upstream error")
		}
	}()

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		event.Status = 413
		failure(w, 413, "Request too large")
		return
	}
	body, req, err := canonical(raw)
	if err != nil {
		failure(w, 400, "Invalid JSON object")
		return
	}
	var model string
	if json.Unmarshal(req["model"], &model) != nil || len(s.cfg.Routes.Models[model]) == 0 {
		failure(w, 400, "Unknown model")
		return
	}
	event.Model = model
	span.SetAttributes(attribute.String("gateway.model", model))

	if len(k.AllowedModels) > 0 {
		permitted := false
		for _, m := range k.AllowedModels {
			if m == model {
				permitted = true
				break
			}
		}
		if !permitted {
			event.Status = 403
			failure(w, 403, "Model not permitted for this key")
			return
		}
	}

	var messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(req["messages"], &messages) != nil || len(messages) == 0 {
		failure(w, 400, "messages must be a nonempty array")
		return
	}
	for _, m := range messages {
		if m.Role == "" {
			failure(w, 400, "message role is required")
			return
		}
	}
	stream := false
	if v, ok := req["stream"]; ok && (string(v) == "null" || json.Unmarshal(v, &stream) != nil) {
		failure(w, 400, "stream must be boolean")
		return
	}
	maxTokens := int64(1024)
	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		if v, ok := req[field]; ok {
			if json.Unmarshal(v, &maxTokens) != nil || maxTokens <= 0 || maxTokens > 1000000 {
				failure(w, 400, "Invalid token limit")
				return
			}
		}
	}
	if _, ok := req["max_tokens"]; !ok {
		if _, ok := req["max_completion_tokens"]; !ok {
			req["max_tokens"] = json.RawMessage(strconv.FormatInt(maxTokens, 10))
			body, _ = json.Marshal(req)
		}
	}
	if _, ok := req["n"]; ok {
		var n int
		if json.Unmarshal(req["n"], &n) != nil || n != 1 {
			failure(w, 400, "Only n=1 is supported")
			return
		}
	}
	var temp float64
	v, hasTemp := req["temperature"]
	deterministic := hasTemp && string(v) != "null" && json.Unmarshal(v, &temp) == nil && temp == 0
	cacheable := (deterministic || r.Header.Get("X-Cache") == "force") && r.Header.Get("X-Cache") != "bypass"
	key := fmt.Sprintf("cache:%s:%x", k.ID, sha256.Sum256(body))
	var result router.Result
	status := "bypass"
	hit := false
	if cacheable {
		result, hit = s.cached(r.Context(), key)
		status = "miss"
	}
	if hit {
		status = "hit"
		event.Saved = result.Cost
		event.Attempts = 0
		s.respond(w, r, stream, result, status, start, 0)
		event.Cache = status
		event.Status = 200
		event.Input = result.Input
		event.Output = result.Output
		return
	}

	// Semantic cache: try to find a sufficiently similar prompt that was
	// already answered. Only active when an embedding endpoint is configured.
	if cacheable && s.semantic != nil && s.cfg.Store != nil {
		var msgsRaw []any
		json.Unmarshal(req["messages"], &msgsRaw)
		promptText := semantic.ExtractText(msgsRaw)
		if promptText != "" {
			if embedding, err := s.semantic.Embed(r.Context(), promptText); err == nil {
				if semKey, ok := s.semantic.Lookup(r.Context(), k.ID, model, embedding); ok {
					if semResult, ok := s.cached(r.Context(), semKey); ok {
						status = "hit"
						event.Saved = semResult.Cost
						event.Cache = "hit"
						event.Status = 200
						event.Input = semResult.Input
						event.Output = semResult.Output
						s.respond(w, r, stream, semResult, "hit", start, 0)
						return
					}
				}
			}
		}
	}

	// Distributed coalescing: try to become the cross-replica leader for this
	// prompt. Followers poll the shared cache rather than issuing their own
	// upstream calls. This is layered on top of process-local coalescing.
	if cacheable && s.cfg.Store != nil {
		lockKey := "distlock:" + key
		acquired, _ := s.cfg.Store.Redis.SetNX(r.Context(), lockKey, reqID, 25*time.Second).Result()
		if !acquired {
			if distResult, ok := s.waitDistributed(r.Context(), key); ok {
				status = "coalesced"
				event.Saved = distResult.Cost
				event.Cache = "coalesced"
				event.Status = 200
				event.Input = distResult.Input
				event.Output = distResult.Output
				event.Provider = "cache"
				s.meter.Coalesced.Inc()
				s.respond(w, r, stream, distResult, "coalesced", start, 0)
				return
			}
			// Timed out: fall through as an independent request.
		} else {
			defer s.cfg.Store.Redis.Del(context.Background(), lockKey)
		}
	}

	// Reserve a conservative upper bound (one token per UTF-8 byte plus output).
	reservedTokens := int64(len(body)) + maxTokens
	reservedCost := s.router.Estimate(model, int64(len(body)), maxTokens)
	bkey := state.BudgetKey(k.ID)
	reserved := false
	if s.cfg.Store != nil {
		ok, remaining, e := s.cfg.Store.Reserve(r.Context(), k, bkey, reservedTokens, reservedCost)
		w.Header().Set("X-Budget-Remaining-Tokens", strconv.FormatInt(remaining, 10))
		w.Header().Set("X-Budget-Reset", time.Now().UTC().Truncate(24*time.Hour).Add(24*time.Hour).Format(time.RFC3339))
		if e != nil {
			event.Status = 503
			failure(w, 503, "Budget store unavailable")
			return
		}
		if !ok {
			s.meter.Budget.Inc()
			event.Status = 402
			failure(w, 402, "Daily budget exceeded")
			// Notify the key's webhook asynchronously.
			if k.WebhookURL != "" {
				s.cfg.Store.SendWebhook(k.WebhookURL, map[string]any{
					"event":     "budget_exceeded",
					"key_id":    k.ID,
					"key_name":  k.Name,
					"timestamp": time.Now().UTC().Format(time.RFC3339),
				})
			}
			return
		}
		reserved = true
	}
	actualTokens, actualCost := int64(0), int64(0)
	defer func() {
		if reserved {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if e := s.cfg.Store.Settle(ctx, bkey, actualTokens-reservedTokens, actualCost-reservedCost); e != nil {
				log.Printf("budget settlement failed; reservation retained for %s: %v", event.ID, e)
			}
		}
	}()
	var f *flight
	leader := true
	if cacheable {
		s.mu.Lock()
		f = s.flights[key]
		if f != nil {
			leader = false
		} else {
			f = &flight{done: make(chan struct{})}
			s.flights[key] = f
		}
		s.mu.Unlock()
	}
	if !leader {
		waitStart := time.Now()
		select {
		case <-r.Context().Done():
			event.Status = 499
			return
		case <-f.done:
			result, err = f.result, f.err
			status = "coalesced"
			event.ProviderMS = float64(time.Since(waitStart).Microseconds()) / 1000
		}
		if err == nil {
			event.Saved = result.Cost
		}
	} else {
		committed := false
		emit := func(frame []byte) error {
			if !committed {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("X-Cache", status)
				w.Header().Set("Trailer", "X-Provider, X-Gateway-Overhead-Ms")
				committed = true
			}
			if _, e := w.Write(frame); e != nil {
				return e
			}
			return http.NewResponseController(w).Flush()
		}
		if cacheable {
			if cached, ok := s.cached(r.Context(), key); ok {
				result = cached
				status = "hit"
				event.Saved = result.Cost
			} else {
				result, err = s.router.Execute(r.Context(), model, body, stream, emit)
			}
		} else {
			result, err = s.router.Execute(r.Context(), model, body, stream, emit)
		}
		event.ProviderMS = result.ProviderMS
		event.Attempts = result.Attempts
		if status == "hit" {
			event.ProviderMS = 0
			event.Attempts = 0
		}
		if err == nil && status != "hit" {
			actualTokens = int64(result.Input + result.Output)
			actualCost = int64(result.Cost*1e6 + 0.999999)
			if actualTokens == 0 {
				actualTokens = reservedTokens
				actualCost = reservedCost
				result.Cost = float64(reservedCost) / 1e6
			}
			event.Cost = result.Cost
			if cacheable {
				s.put(r.Context(), key, result, k.TTL)
				if s.semantic != nil && s.cfg.Store != nil {
					var msgsRaw []any
					json.Unmarshal(req["messages"], &msgsRaw)
					if promptText := semantic.ExtractText(msgsRaw); promptText != "" {
						if emb, embErr := s.semantic.Embed(context.Background(), promptText); embErr == nil {
							s.semantic.Store(context.Background(), k.ID, model, key, emb)
						}
					}
				}
			}
		} else if committed {
			// Use partial token data from the stream when available; otherwise
			// retain the reservation conservatively since upstream may have billed.
			if result.Input > 0 || result.Output > 0 {
				actualTokens = int64(result.Input + result.Output)
				actualCost = int64(result.Cost*1e6 + 0.999999)
			} else {
				actualTokens = reservedTokens
				actualCost = reservedCost
			}
		}
		if f != nil {
			s.mu.Lock()
			f.result, f.err = result, err
			delete(s.flights, key)
			close(f.done)
			s.mu.Unlock()
		}
		if committed {
			w.Header().Set("X-Provider", result.Provider)
			w.Header().Set("X-Gateway-Overhead-Ms", strconv.FormatFloat(max(0, float64(time.Since(start).Microseconds())/1000-result.ProviderMS), 'f', 3, 64))
			event.Cache = status
			event.Provider = result.Provider
			event.Input = result.Input
			event.Output = result.Output
			if err != nil {
				event.Status = 502
			} else {
				event.Status = 200
			}
			return
		}
	}
	if err != nil {
		event.Status = 503
		w.Header().Set("Retry-After", "1")
		failure(w, 503, "No provider could complete the request")
		return
	}
	event.Cache = status
	event.Provider = result.Provider
	event.Input = result.Input
	event.Output = result.Output
	event.Status = 200
	s.respond(w, r, stream, result, status, start, event.ProviderMS)
}
func (s *Server) respond(w http.ResponseWriter, r *http.Request, stream bool, v router.Result, status string, start time.Time, providerMS float64) {
	w.Header().Set("X-Cache", status)
	provider := v.Provider
	if status == "hit" || status == "coalesced" {
		provider = "cache"
	}
	w.Header().Set("X-Provider", provider)
	w.Header().Set("X-Gateway-Overhead-Ms", strconv.FormatFloat(max(0, float64(time.Since(start).Microseconds())/1000-providerMS), 'f', 3, 64))
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		for _, b := range v.Frames {
			if _, e := w.Write(b); e != nil {
				return
			}
			if http.NewResponseController(w).Flush() != nil {
				return
			}
		}
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(v.Body)
	}
}
func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	if !equal(bearer(r), s.cfg.AdminKey) {
		failure(w, 401, "Invalid admin key")
		return
	}
	if s.cfg.Store == nil {
		failure(w, 503, "Persistent storage required")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/admin/keys" && r.Method == "POST":
		k := state.Key{RPM: 60, DailyTokens: 1000000, DailyMicros: 10000000, TTL: 86400}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		dec.DisallowUnknownFields()
		if dec.Decode(&k) != nil || k.Name == "" || k.RPM <= 0 || k.DailyTokens <= 0 || k.DailyMicros <= 0 || k.TTL <= 0 || k.TTL > 604800 {
			failure(w, 400, "Invalid key limits or name")
			return
		}
		for _, m := range k.AllowedModels {
			if m == "" {
				failure(w, 400, "allowed_models entries must be non-empty")
				return
			}
		}
		if k.ExpiresAt != nil && !k.ExpiresAt.After(time.Now()) {
			failure(w, 400, "expires_at must be in the future")
			return
		}
		if k.WebhookURL != "" && !strings.HasPrefix(k.WebhookURL, "http") {
			failure(w, 400, "webhook_url must be an HTTP(S) URL")
			return
		}
		k, raw, err := s.cfg.Store.Create(r.Context(), k)
		if err != nil {
			failure(w, 503, "Key creation failed")
			return
		}
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(map[string]any{"key": raw, "limits": k})
	case strings.HasPrefix(r.URL.Path, "/admin/keys/") && r.Method == "DELETE":
		if err := s.cfg.Store.Revoke(r.Context(), strings.TrimPrefix(r.URL.Path, "/admin/keys/")); err != nil {
			failure(w, 404, "Key not found")
			return
		}
		w.WriteHeader(204)
	case r.URL.Path == "/admin/usage" && r.Method == "GET":
		s.usage(w, r)
	default:
		failure(w, 404, "Unknown admin endpoint")
	}
}
func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()
	var err error
	if v := r.URL.Query().Get("from"); v != "" {
		from, err = time.Parse(time.RFC3339, v)
		if err != nil {
			failure(w, 400, "Invalid from timestamp")
			return
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		to, err = time.Parse(time.RFC3339, v)
		if err != nil {
			failure(w, 400, "Invalid to timestamp")
			return
		}
	}
	if !to.After(from) {
		failure(w, 400, "Invalid time range")
		return
	}
	if r.URL.Query().Get("group_by") == "key" {
		rows, err := s.cfg.Store.DB.Query(r.Context(),
			`SELECT k.name, count(*), COALESCE(sum(u.cost_usd),0)::float8, COALESCE(sum(u.saved_usd),0)::float8
			 FROM usage u JOIN api_keys k ON k.id=u.api_key_id
			 WHERE u.created_at>=$1 AND u.created_at<$2
			 GROUP BY k.name ORDER BY k.name`, from, to)
		if err != nil {
			failure(w, 503, "Usage storage unavailable")
			return
		}
		defer rows.Close()
		type keyRow struct {
			KeyName  string  `json:"key_name"`
			Requests int64   `json:"requests"`
			CostUSD  float64 `json:"cost_usd"`
			SavedUSD float64 `json:"saved_usd"`
		}
		out := []keyRow{}
		for rows.Next() {
			var kr keyRow
			if err := rows.Scan(&kr.KeyName, &kr.Requests, &kr.CostUSD, &kr.SavedUSD); err != nil {
				continue
			}
			out = append(out, kr)
		}
		json.NewEncoder(w).Encode(out)
		return
	}

	var count, input, output int64
	var cost, saved, latency, overhead float64
	err = s.cfg.Store.DB.QueryRow(r.Context(), `SELECT count(*),COALESCE(sum(input_tokens),0),COALESCE(sum(output_tokens),0),COALESCE(sum(cost_usd),0)::float8,COALESCE(sum(saved_usd),0)::float8,COALESCE(percentile_cont(0.99) WITHIN GROUP(ORDER BY provider_ms+overhead_ms),0),COALESCE(percentile_cont(0.99) WITHIN GROUP(ORDER BY overhead_ms),0) FROM usage WHERE created_at>=$1 AND created_at<$2 AND ($3='' OR api_key_id::text=$3)`, from, to, r.URL.Query().Get("key")).Scan(&count, &input, &output, &cost, &saved, &latency, &overhead)
	if err != nil {
		failure(w, 503, "Usage storage unavailable")
		return
	}
	pct := 0.0
	if cost+saved > 0 {
		pct = 100 * saved / (cost + saved)
	}
	json.NewEncoder(w).Encode(map[string]any{"requests": count, "input_tokens": input, "output_tokens": output, "cost_usd": cost, "saved_usd": saved, "cost_saved_pct": pct, "p99_latency_ms": latency, "p99_overhead_ms": overhead, "from": from, "to": to})
}
