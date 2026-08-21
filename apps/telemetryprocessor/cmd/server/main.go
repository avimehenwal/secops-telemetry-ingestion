package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/analytics"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/api"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/config"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/enrichment"
	"github.com/avimehenwal/secops-telemetry-ingestion/apps/telemetryprocessor/internal/logging"
)

func main() {
	env := config.Environment()
	format := logging.ResolveFormat(os.Getenv("LOG_FORMAT"), config.IsLocal(env))
	root := logging.New(os.Stdout, logLevel(), format, env)
	logger := logging.Component(root, logging.ComponentApp)
	enrichmentLog := logging.Component(root, logging.ComponentUpstreamEnrichment)
	analyticsLog := logging.Component(root, logging.ComponentUpstreamAnalytics)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration error", "error", err.Error())
		os.Exit(1)
	}

	analyticsClient := analytics.NewClient(cfg.AnalyticsURL, cfg.APIKey, analytics.Options{
		Timeout:      cfg.AnalyticsTimeout,
		RateRequests: cfg.AnalyticsRateRequests,
		RateWindow:   cfg.AnalyticsRateWindow,
		MaxRetries:   cfg.AnalyticsMaxRetries,
		Logger:       analyticsLog,
	})

	batcher := analytics.NewBatcher(analyticsClient.SendBatch, analytics.BatcherOptions{
		BatchSize:   cfg.AnalyticsBatchSize,
		MaxFillWait: cfg.AnalyticsMaxFillWait,
		QueueSize:   cfg.AnalyticsQueueSize,
		DrainGrace:  cfg.AnalyticsDrainGrace,
		Logger:      analyticsLog,
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
			Logger:           enrichmentLog,
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
		logger.Info("telemetryprocessor listening",
			"logFormat", string(format),
			"addr", cfg.Addr,
			"enrichmentURL", cfg.EnrichmentURL,
			"analyticsURL", cfg.AnalyticsURL,
			"rateRequests", cfg.AnalyticsRateRequests,
			"rateWindow", cfg.AnalyticsRateWindow.String(),
			"batchSize", cfg.AnalyticsBatchSize)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err.Error())
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining connections")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err.Error())
	}

	stopBatcher()
	batcher.Wait()
	logger.Info("stopped")
}

func logLevel() slog.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
