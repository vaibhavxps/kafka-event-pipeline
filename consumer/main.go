package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// Event mirrors the producer's payload structure for deserialization.
type Event struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

// Prometheus metric: counts every successfully processed Kafka message.
var messagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "kafka_messages_processed_total",
	Help: "Total number of Kafka messages successfully processed by the consumer.",
})

func init() {
	prometheus.MustRegister(messagesProcessed)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// --- Configuration via environment variables ---
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events-topic"
	}

	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "pipeline-consumer-group"
	}

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":8080"
	}

	slog.Info("starting consumer",
		"brokers", brokerList,
		"topic", topic,
		"group_id", groupID,
		"metrics_addr", metricsAddr,
	)

	// --- Kafka Reader (Consumer Group) ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,            // fetch as soon as 1 byte is available
		MaxBytes:       10e6,         // 10 MB max per fetch
		CommitInterval: time.Second,  // auto-commit offsets every second
		StartOffset:    kafka.FirstOffset,
	})

	// --- Context + Signal Handling ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig.String())
		cancel()
	}()

	// --- HTTP Server for Prometheus /metrics ---
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:    metricsAddr,
		Handler: mux,
	}

	// Start the HTTP server in a goroutine.
	go func() {
		slog.Info("metrics server starting", "addr", metricsAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
			cancel() // bring down the consumer loop if the server dies
		}
	}()

	// --- Kafka Consume Loop ---
	// Runs in the main goroutine; blocks until context is cancelled.
	consumeLoop(ctx, reader)

	// --- Graceful Shutdown ---
	slog.Info("shutting down metrics server")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown error", "error", err)
	}

	slog.Info("closing kafka reader")
	if err := reader.Close(); err != nil {
		slog.Error("kafka reader close error", "error", err)
	}

	slog.Info("consumer shut down cleanly")
}

// consumeLoop continuously reads messages from Kafka, simulates processing,
// and increments the Prometheus counter.
func consumeLoop(ctx context.Context, reader *kafka.Reader) {
	for {
		// FetchMessage blocks until a message is available or context is cancelled.
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("consume loop stopped (context cancelled)")
				return
			}
			slog.Error("error fetching message", "error", err)
			continue
		}

		// Deserialize the event payload.
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal event",
				"error", err,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			// Still commit so we don't re-read a permanently bad message.
			if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
				slog.Error("failed to commit bad message", "error", commitErr)
			}
			continue
		}

		// Simulate processing time.
		time.Sleep(200 * time.Millisecond)

		// Commit the offset after successful processing.
		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("failed to commit message",
				"error", err,
				"id", event.ID,
			)
			continue
		}

		// Increment the Prometheus counter.
		messagesProcessed.Inc()

		slog.Info("message processed",
			"id", event.ID,
			"data", event.Data,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
	}
}
