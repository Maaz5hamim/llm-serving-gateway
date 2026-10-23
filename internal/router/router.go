package router

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"llm-serving-gateway/internal/telemetry"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
)

type Provider struct {
	Name        string  `yaml:"provider"`
	Kind        string  `yaml:"kind"`
	URL         string  `yaml:"url"`
	KeyEnv      string  `yaml:"key_env"`
	Model       string  `yaml:"model"`
	InputPrice  float64 `yaml:"input_per_mtok"`
	OutputPrice float64 `yaml:"output_per_mtok"`
}
type Config struct {
	Models    map[string][]Provider `yaml:"models"`
	TimeoutMS int                   `yaml:"timeout_ms"`
	// Hedge sends the request to the first two available providers in parallel
	// for non-streaming calls and uses whichever responds first. This reduces
	// tail latency at the cost of occasionally duplicating provider work.
	Hedge bool `yaml:"hedge"`
}
type breaker struct {
	history  []bool
	timeouts int
	opened   time.Time
	probe    bool
}
type Router struct {
	Config   Config
	client   *http.Client
	mu       sync.Mutex
	breakers map[string]*breaker
	meter    *telemetry.Meter
}
type Result struct {
	Body                    []byte   `json:"body"`
	Frames                  [][]byte `json:"frames,omitempty"`
	Provider                string   `json:"provider"`
	Input, Output, Attempts int
	Cost                    float64
	ProviderMS              float64
}

func Load(path string) (Config, error) {
	b, e := os.ReadFile(path)
	var c Config
	if e == nil {
		e = yaml.Unmarshal(b, &c)
	}
	if e == nil {
		e = c.Validate()
	}
	return c, e
}
func (c Config) Validate() error {
	if len(c.Models) == 0 {
		return errors.New("no model routes")
	}
	for alias, ps := range c.Models {
		if alias == "" || len(ps) == 0 {
			return errors.New("empty route")
		}
		for _, p := range ps {
			if p.Name == "" || p.Model == "" || !strings.HasPrefix(p.URL, "http") || p.InputPrice < 0 || p.OutputPrice < 0 {
				return errors.New("invalid provider")
			}
			if p.Kind != "openai" && p.Kind != "anthropic" {
				return errors.New("unknown provider kind")
			}
		}
	}
	return nil
}
func New(c Config, m *telemetry.Meter) *Router {
	if c.TimeoutMS <= 0 {
		c.TimeoutMS = 10000
	}
	return &Router{Config: c, client: &http.Client{}, breakers: map[string]*breaker{}, meter: m}
}
func (r *Router) allow(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.breakers[name]
	if b == nil {
		b = &breaker{}
		r.breakers[name] = b
	}
	if b.opened.IsZero() {
		r.meter.Breaker.WithLabelValues(name).Set(0)
		return true
	}
	if time.Since(b.opened) < 30*time.Second || b.probe {
		return false
	}
	b.probe = true
	r.meter.Breaker.WithLabelValues(name).Set(2)
	return true
}
func (r *Router) finish(name string, err error) {
	// Explicit cancellation (e.g. hedge winner cancels the loser) must not
	// penalise the circuit breaker — only real failures count.
	if errors.Is(err, context.Canceled) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.breakers[name]
	if b == nil {
		return
	}
	if b.probe {
		b.probe = false
		if err == nil {
			b.opened = time.Time{}
			b.history = nil
			b.timeouts = 0
			r.meter.Breaker.WithLabelValues(name).Set(0)
			return
		}
		b.opened = time.Now()
		r.meter.Breaker.WithLabelValues(name).Set(1)
		return
	}
	if err == nil {
		b.timeouts = 0
	} else if errors.Is(err, context.DeadlineExceeded) {
		b.timeouts++
	} else {
		b.timeouts = 0
	}
	b.history = append(b.history, err != nil)
	if len(b.history) > 20 {
		b.history = b.history[1:]
	}
	n := 0
	for _, bad := range b.history {
		if bad {
			n++
		}
	}
	if b.timeouts >= 5 || (len(b.history) == 20 && n > 10) {
		b.opened = time.Now()
		r.meter.Breaker.WithLabelValues(name).Set(1)
	}
}
func (r *Router) Estimate(alias string, input, output int64) int64 {
	var cost float64
	for _, p := range r.Config.Models[alias] {
		cost = max(cost, float64(input)*p.InputPrice+float64(output)*p.OutputPrice)
	}
	return int64(cost + 0.999999)
}

type upstreamError struct{ status int }

func (e upstreamError) Error() string { return fmt.Sprintf("upstream status %d", e.status) }
func retryable(err error) bool {
	var e upstreamError
	if errors.As(err, &e) {
		return e.status == 429 || e.status >= 500
	}
	return true
}

func (r *Router) Execute(ctx context.Context, alias string, raw []byte, stream bool, emit func([]byte) error) (out Result, err error) {
	if r.Config.Hedge && !stream {
		return r.executeHedged(ctx, alias, raw)
	}
	providerMS := 0.0
	defer func() { out.ProviderMS = providerMS }()
	previous := ""
	for _, p := range r.Config.Models[alias] {
		if !r.allow(p.Name) {
			continue
		}
		if previous != "" {
			r.meter.Failovers.WithLabelValues(previous, p.Name).Inc()
		}
		previous = p.Name
		for attempt := 0; attempt < 2; attempt++ {
			out.Attempts++
			attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(r.Config.TimeoutMS)*time.Millisecond)
			sent := false
			wrapped := func(b []byte) error { sent = true; return emit(b) }
			callStart := time.Now()
			v, e := r.call(attemptCtx, p, alias, raw, stream, wrapped)
			providerMS += float64(time.Since(callStart).Microseconds()) / 1000
			cancel()
			r.finish(p.Name, e)
			if e == nil {
				v.Attempts = out.Attempts
				return v, nil
			}
			out.Provider = v.Provider
			out.Input = v.Input
			out.Output = v.Output
			out.Cost = v.Cost
			err = e
			r.meter.Errors.WithLabelValues(p.Name, "upstream").Inc()
			if ctx.Err() != nil {
				return out, ctx.Err()
			}
			if sent {
				return out, e
			}
			if !retryable(e) {
				return out, e
			}
			if attempt == 0 {
				select {
				case <-ctx.Done():
					return out, ctx.Err()
				case <-time.After(time.Duration(25+rand.IntN(50)) * time.Millisecond):
				}
			}
			if attempt == 0 && !r.allow(p.Name) {
				break
			}
		}
	}
	if err == nil {
		err = errors.New("all providers have open circuits")
	}
	return out, err
}

type hedgeOutcome struct {
	result Result
	err    error
	pMS    float64
}

// executeHedged sends the request to the first two eligible providers
// simultaneously and uses whichever responds first. The slower goroutine is
// cancelled; its context.Canceled does not affect the circuit breaker.
func (r *Router) executeHedged(ctx context.Context, alias string, raw []byte) (Result, error) {
	providers := r.Config.Models[alias]
	var eligible []Provider
	for _, p := range providers {
		if r.allow(p.Name) {
			eligible = append(eligible, p)
		}
		if len(eligible) == 2 {
			break
		}
	}
	if len(eligible) < 2 {
		// Fall back to sequential execution when fewer than two providers are open.
		return r.Execute(ctx, alias, raw, false, func([]byte) error { return nil })
	}

	hedgeCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	ch := make(chan hedgeOutcome, 2)
	for _, p := range eligible {
		p := p
		go func() {
			pCtx, cancel := context.WithTimeout(hedgeCtx, time.Duration(r.Config.TimeoutMS)*time.Millisecond)
			defer cancel()
			callStart := time.Now()
			v, e := r.call(pCtx, p, alias, raw, false, func([]byte) error { return nil })
			pMS := float64(time.Since(callStart).Microseconds()) / 1000
			r.finish(p.Name, e)
			if e != nil {
				r.meter.Errors.WithLabelValues(p.Name, "upstream").Inc()
			}
			ch <- hedgeOutcome{v, e, pMS}
		}()
	}

	first := <-ch
	cancelAll() // cancel the slower goroutine
	<-ch        // drain to avoid goroutine leak

	if first.err == nil {
		first.result.Attempts = 1
		first.result.ProviderMS = first.pMS
		return first.result, nil
	}
	// Both failed; return whatever the first sent.
	return first.result, first.err
}

func (r *Router) call(ctx context.Context, p Provider, alias string, raw []byte, stream bool, emit func([]byte) error) (out Result, err error) {
	ctx, span := otel.Tracer("gateway").Start(ctx, "provider.call")
	span.SetAttributes(
		attribute.String("provider.name", p.Name),
		attribute.String("provider.kind", p.Kind),
		attribute.String("provider.model", p.Model),
		attribute.Bool("provider.stream", stream),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
		span.End()
	}()

	var req map[string]any
	json.Unmarshal(raw, &req)
	req["model"] = p.Model
	path := "/chat/completions"
	if p.Kind == "anthropic" {
		path = "/messages"
		if err = anthropicRequest(req); err != nil {
			return out, upstreamError{400}
		}
	} else if stream {
		req["stream_options"] = map[string]any{"include_usage": true}
	}
	body, _ := json.Marshal(req)
	request, e := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(p.URL, "/")+path, bytes.NewReader(body))
	if e != nil {
		return out, e
	}
	request.Header.Set("Content-Type", "application/json")
	key := os.Getenv(p.KeyEnv)
	if p.Kind == "anthropic" {
		request.Header.Set("x-api-key", key)
		request.Header.Set("anthropic-version", "2023-06-01")
	} else if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	resp, e := r.client.Do(request)
	if e != nil {
		return out, e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, upstreamError{resp.StatusCode}
	}
	out.Provider = p.Name
	if stream {
		if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			return out, errors.New("invalid stream content type")
		}
		err = readStream(resp.Body, p, alias, &out, emit)
	} else {
		out.Body, err = io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
		if err == nil && (len(out.Body) > 16<<20 || !json.Valid(out.Body)) {
			err = errors.New("invalid provider response")
		}
		if err == nil {
			if p.Kind == "anthropic" {
				out.Body, err = anthropicResponse(out.Body, alias)
			}
			var v struct {
				Usage struct {
					Input  int `json:"prompt_tokens"`
					Output int `json:"completion_tokens"`
				}
			}
			json.Unmarshal(out.Body, &v)
			out.Input = v.Usage.Input
			out.Output = v.Usage.Output
		}
	}
	out.Cost = (float64(out.Input)*p.InputPrice + float64(out.Output)*p.OutputPrice) / 1e6
	span.SetAttributes(
		attribute.Int("provider.input_tokens", out.Input),
		attribute.Int("provider.output_tokens", out.Output),
	)
	return out, err
}
func readStream(reader io.Reader, p Provider, alias string, out *Result, emit func([]byte) error) error {
	scan := bufio.NewScanner(reader)
	scan.Buffer(make([]byte, 4096), 1<<20)
	size := 0
	done := false
	id := "chatcmpl-anthropic"
	for scan.Scan() {
		line := scan.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			done = true
			break
		}
		var v map[string]any
		if json.Unmarshal([]byte(data), &v) != nil {
			return errors.New("invalid SSE JSON")
		}
		if p.Kind == "anthropic" {
			typ, _ := v["type"].(string)
			switch typ {
			case "error":
				return errors.New("anthropic stream error")
			case "message_start":
				msg, _ := v["message"].(map[string]any)
				if s, ok := msg["id"].(string); ok {
					id = s
				}
				usage, _ := msg["usage"].(map[string]any)
				out.Input = number(usage["input_tokens"])
				data = chunk(id, alias, map[string]any{"role": "assistant"}, nil)
			case "content_block_delta":
				delta, _ := v["delta"].(map[string]any)
				if delta["type"] != "text_delta" {
					return errors.New("unsupported anthropic stream delta")
				}
				data = chunk(id, alias, map[string]any{"content": delta["text"]}, nil)
			case "message_delta":
				usage, _ := v["usage"].(map[string]any)
				out.Output = number(usage["output_tokens"])
				delta, _ := v["delta"].(map[string]any)
				reason := "stop"
				if delta["stop_reason"] == "max_tokens" {
					reason = "length"
				}
				data = chunk(id, alias, map[string]any{}, reason)
			case "message_stop":
				done = true
				continue
			default:
				continue
			}
		} else {
			if _, ok := v["error"]; ok {
				return errors.New("provider stream error")
			}
			usage, _ := v["usage"].(map[string]any)
			if usage != nil {
				out.Input = number(usage["prompt_tokens"])
				out.Output = number(usage["completion_tokens"])
			}
		}
		frame := []byte("data: " + data + "\n\n")
		size += len(frame)
		if size > 16<<20 {
			return errors.New("stream too large")
		}
		out.Frames = append(out.Frames, frame)
		if err := emit(frame); err != nil {
			return err
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if !done {
		return errors.New("incomplete provider stream")
	}
	frame := []byte("data: [DONE]\n\n")
	out.Frames = append(out.Frames, frame)
	return emit(frame)
}
func number(v any) int { f, _ := v.(float64); return max(0, int(f)) }
func chunk(id, model string, delta map[string]any, finish any) string {
	b, _ := json.Marshal(map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}})
	return string(b)
}
func anthropicRequest(req map[string]any) error {
	for _, key := range []string{"tools", "tool_choice", "response_format", "seed", "logprobs", "n"} {
		if _, ok := req[key]; ok {
			return fmt.Errorf("unsupported anthropic parameter %s", key)
		}
	}
	msgs, _ := req["messages"].([]any)
	var system []string
	var rest []any
	for _, m := range msgs {
		v, ok := m.(map[string]any)
		if !ok {
			return errors.New("invalid message")
		}
		role, _ := v["role"].(string)
		if role == "system" {
			text, ok := v["content"].(string)
			if !ok {
				return errors.New("system content must be text")
			}
			system = append(system, text)
		} else if role == "user" || role == "assistant" {
			if _, ok := v["content"].(string); !ok {
				return errors.New("anthropic supports text messages only")
			}
			rest = append(rest, v)
		} else {
			return errors.New("unsupported message role")
		}
	}
	req["messages"] = rest
	if len(system) > 0 {
		req["system"] = strings.Join(system, "\n\n")
	}
	if req["max_tokens"] == nil {
		req["max_tokens"] = 1024
	}
	delete(req, "stream_options")
	return nil
}
func anthropicResponse(raw []byte, alias string) ([]byte, error) {
	var v struct {
		ID         string
		Content    []struct{ Type, Text string }
		StopReason string `json:"stop_reason"`
		Usage      struct {
			Input  int `json:"input_tokens"`
			Output int `json:"output_tokens"`
		}
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	text := ""
	for _, c := range v.Content {
		if c.Type != "text" {
			return nil, errors.New("unsupported anthropic content")
		}
		text += c.Text
	}
	reason := "stop"
	if v.StopReason == "max_tokens" {
		reason = "length"
	}
	return json.Marshal(map[string]any{"id": v.ID, "object": "chat.completion", "created": time.Now().Unix(), "model": alias, "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": text}, "finish_reason": reason}}, "usage": map[string]int{"prompt_tokens": v.Usage.Input, "completion_tokens": v.Usage.Output, "total_tokens": v.Usage.Input + v.Usage.Output}})
}
