package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/api"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/config"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf("configuration error: %v", err)
	}

	analyticsClient := analytics.NewClient(cfg.AnalyticsURL, cfg.APIKey, analytics.Options{
		Timeout:      cfg.AnalyticsTimeout,
		RateRequests: cfg.AnalyticsRateRequests,
		RateWindow:   cfg.AnalyticsRateWindow,
		MaxRetries:   cfg.AnalyticsMaxRetries,
		Logger:       logger,
	})

	batcher := analytics.NewBatcher(analyticsClient.SendBatch, analytics.BatcherOptions{
		BatchSize:   cfg.AnalyticsBatchSize,
		MaxFillWait: cfg.AnalyticsMaxFillWait,
		QueueSize:   cfg.AnalyticsQueueSize,
		DrainGrace:  cfg.AnalyticsDrainGrace,
		Logger:      logger,
	})
	batcherCtx, stopBatcher := context.WithCancel(context.Background())
	defer stopBatcher()
	go batcher.Run(batcherCtx)

	handler := &api.IngestHandler{
		Enrichment: enrichment.NewClient(cfg.EnrichmentURL, cfg.APIKey, enrichment.Options{
			Timeout:          cfg.EnrichmentTimeout,
			MaxAttempts:      cfg.EnrichmentMaxAttempts,
			BackoffBase:      cfg.EnrichmentBackoffBase,
			BackoffMax:       cfg.EnrichmentBackoffMax,
			BreakerThreshold: cfg.EnrichmentBreakerThreshold,
			BreakerCooldown:  cfg.EnrichmentBreakerCooldown,
			Logger:           logger,
		}),
		Analytics: batcher,
		Logger:    logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Status    string `json:"status"`
			Timestamp string `json:"timestamp"`
		}{
			Status:    "ok",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.Handle("/ingest", handler)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.LoggingMiddleware(logger, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Printf("telemetryprocessor listening on %s (enrichment=%s analytics=%s, rate=%d req/%s, batch=%d)",
			cfg.Addr, cfg.EnrichmentURL, cfg.AnalyticsURL, cfg.AnalyticsRateRequests, cfg.AnalyticsRateWindow, cfg.AnalyticsBatchSize)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Println("shutdown signal received, draining connections...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("graceful shutdown failed: %v", err)
	}

	stopBatcher()
	batcher.Wait()
	logger.Println("stopped")
}
