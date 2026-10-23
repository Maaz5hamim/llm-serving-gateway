package telemetry

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	ID, Key, Model, Provider, Cache     string
	Input, Output, Status, Attempts     int
	Cost, Saved, ProviderMS, OverheadMS float64
}
type Meter struct {
	Registry                   *prometheus.Registry
	Requests                   *prometheus.CounterVec
	Duration                   *prometheus.HistogramVec
	Overhead                   prometheus.Histogram
	Cost, Saved, Dropped       prometheus.Counter
	Errors                     *prometheus.CounterVec
	Failovers                  *prometheus.CounterVec
	Breaker                    *prometheus.GaugeVec
	Limited, Budget, Coalesced prometheus.Counter
	queue                      chan Event
	stop                       chan struct{}
	wg                         sync.WaitGroup
	db                         *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Meter {
	m := &Meter{Registry: prometheus.NewRegistry(), db: db, queue: make(chan Event, 10000), stop: make(chan struct{})}
	m.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_requests_total", Help: "Requests by cache outcome"}, []string{"cache_status"})
	m.Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_request_duration_seconds", Help: "Request duration", Buckets: prometheus.DefBuckets}, []string{"cache", "provider", "status"})
	m.Overhead = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gateway_overhead_seconds", Help: "Elapsed duration excluding time waiting for providers", Buckets: []float64{.0001, .0005, .001, .002, .005, .01, .02, .05, .1, .5, 1}})
	m.Cost = counter("gateway_cost_usd_total")
	m.Saved = counter("gateway_saved_usd_total")
	m.Dropped = counter("gateway_usage_dropped_total")
	m.Limited = counter("gateway_ratelimited_total")
	m.Budget = counter("gateway_budget_rejected_total")
	m.Coalesced = counter("gateway_coalesced_total")
	m.Errors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_provider_errors_total", Help: "Provider failures"}, []string{"provider", "kind"})
	m.Failovers = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_failovers_total", Help: "Provider failovers"}, []string{"from", "to"})
	m.Breaker = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gateway_breaker_state", Help: "0 closed, 1 open, 2 half open"}, []string{"provider"})
	m.Registry.MustRegister(m.Requests, m.Duration, m.Overhead, m.Cost, m.Saved, m.Dropped, m.Errors, m.Failovers, m.Breaker, m.Limited, m.Budget, m.Coalesced)
	if db != nil {
		m.wg.Add(1)
		go m.run()
	}
	return m
}
func counter(n string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{Name: n, Help: n})
}
func (m *Meter) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Meter) Record(e Event) {
	m.Requests.WithLabelValues(e.Cache).Inc()
	m.Duration.WithLabelValues(e.Cache, e.Provider, http.StatusText(e.Status)).Observe((e.ProviderMS + e.OverheadMS) / 1000)
	m.Overhead.Observe(e.OverheadMS / 1000)
	m.Cost.Add(max(0, e.Cost))
	m.Saved.Add(max(0, e.Saved))
	if e.Cache == "coalesced" {
		m.Coalesced.Inc()
	}
	if m.db != nil {
		select {
		case m.queue <- e:
		default:
			m.Dropped.Inc()
		}
	}
}
func (m *Meter) Close() {
	if m.db != nil {
		close(m.stop)
		m.wg.Wait()
	}
}
func (m *Meter) run() {
	defer m.wg.Done()
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	batch := make([]Event, 0, 200)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		b := &pgx.Batch{}
		for _, e := range batch {
			var key any
			if e.Key != "" {
				key = e.Key
			}
			b.Queue(`INSERT INTO usage(request_id,api_key_id,model_alias,provider,cache_status,input_tokens,output_tokens,cost_usd,saved_usd,provider_ms,overhead_ms,status,attempts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, e.ID, key, e.Model, e.Provider, e.Cache, e.Input, e.Output, e.Cost, e.Saved, e.ProviderMS, e.OverheadMS, e.Status, e.Attempts)
		}
		tx, err := m.db.Begin(ctx)
		if err != nil {
			return false
		}
		defer tx.Rollback(ctx)
		results := tx.SendBatch(ctx, b)
		err = results.Close()
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			log.Printf("usage flush: %v", err)
			return false
		}
		batch = batch[:0]
		return true
	}
	for {
		select {
		case e := <-m.queue:
			batch = append(batch, e)
			if len(batch) >= 200 {
				flush()
			}
			if len(batch) >= 10000 {
				m.Dropped.Add(float64(len(batch)))
				batch = batch[:0]
			}
		case <-timer.C:
			flush()
		case <-m.stop:
			for {
				select {
				case e := <-m.queue:
					batch = append(batch, e)
				default:
					// Retry the final flush with exponential backoff so that a
					// transient DB error during shutdown does not silently drop
					// analytics. Max additional wait: ~3.5 s, well within the
					// 15 s server shutdown window.
					for attempt := 0; attempt < 3; attempt++ {
						if flush() {
							return
						}
						if attempt < 2 {
							time.Sleep(time.Duration(1<<uint(attempt)) * 500 * time.Millisecond)
						}
					}
					m.Dropped.Add(float64(len(batch)))
					return
				}
			}
		}
	}
}
