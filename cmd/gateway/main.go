package main

import (
	"context"
	"llm-serving-gateway/db/migrations"
	"llm-serving-gateway/internal/gateway"
	"llm-serving-gateway/internal/router"
	"llm-serving-gateway/internal/semantic"
	"llm-serving-gateway/internal/state"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// initTracing sets up an OTLP HTTP trace exporter when
// OTEL_EXPORTER_OTLP_ENDPOINT is set, and returns a shutdown function.
// If the env var is absent, a no-op tracer is used and the function is a
// cheap no-op, so callers always defer it unconditionally.
func initTracing(ctx context.Context) func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func() {}
	}
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Printf("otel exporter: %v", err)
		return func() {}
	}
	res, _ := sdkresource.New(ctx,
		sdkresource.WithFromEnv(),
		sdkresource.WithProcess(),
		sdkresource.WithHost(),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return func() {
		sCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(sCtx); err != nil {
			log.Printf("otel shutdown: %v", err)
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing := initTracing(ctx)
	defer shutdownTracing()

	cfg := gateway.Config{
		APIKey:   os.Getenv("GATEWAY_API_KEY"),
		AdminKey: os.Getenv("ADMIN_API_KEY"),
	}

	rpm, err := strconv.Atoi(env("RPM_LIMIT", "60"))
	if err != nil || rpm <= 0 {
		log.Fatal("RPM_LIMIT must be positive")
	}
	cfg.RPM = rpm

	if path := env("MODELS_CONFIG", "config/models.yaml"); path != "" {
		cfg.Routes, err = router.Load(path)
		if err != nil {
			log.Fatal(err)
		}
	}

	if os.Getenv("DATABASE_URL") != "" {
		openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		cfg.Store, err = state.Open(openCtx, env("REDIS_URL", "redis://localhost:6379"), os.Getenv("DATABASE_URL"))
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		defer cfg.Store.Close()
		if cfg.AdminKey == "" {
			log.Fatal("ADMIN_API_KEY is required with persistent storage")
		}

		migCtx, migCancel := context.WithTimeout(ctx, 30*time.Second)
		if err = migrations.Run(migCtx, cfg.Store.DB); err != nil {
			migCancel()
			log.Fatal("migrations: ", err)
		}
		migCancel()
	} else if cfg.APIKey == "" {
		log.Fatal("GATEWAY_API_KEY is required in local mode")
	}

	if cfg.Store != nil {
		priceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		for _, ps := range cfg.Routes.Models {
			for _, p := range ps {
				_, err = cfg.Store.DB.Exec(priceCtx,
					`INSERT INTO model_prices(provider,model,input_per_mtok,output_per_mtok) VALUES($1,$2,$3,$4) ON CONFLICT(provider,model) DO UPDATE SET input_per_mtok=EXCLUDED.input_per_mtok,output_per_mtok=EXCLUDED.output_per_mtok`,
					p.Name, p.Model, p.InputPrice, p.OutputPrice)
				if err != nil {
					cancel()
					log.Fatal(err)
				}
			}
		}
		cancel()
	}

	// Optional semantic cache: activated when EMBEDDING_URL is set.
	if embURL := os.Getenv("EMBEDDING_URL"); embURL != "" {
		cfg.Semantic = &semantic.Config{
			URL:       embURL,
			Model:     env("EMBEDDING_MODEL", "text-embedding-3-small"),
			KeyEnv:    env("EMBEDDING_KEY_ENV", "EMBEDDING_API_KEY"),
			Threshold: 0.95,
		}
	}

	g := gateway.New(cfg)
	defer g.Close()

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           g.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(c); err != nil {
			log.Printf("shutdown: %v", err)
			srv.Close()
		}
	}()

	log.Printf("Gateway listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-shutdownDone
}
