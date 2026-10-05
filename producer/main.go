package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event represents the JSON payload pushed to Kafka.
type Event struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

// mockData holds sample strings used to generate realistic-looking event payloads.
var mockData = []string{
	"user_signup",
	"page_view",
	"purchase_completed",
	"item_added_to_cart",
	"session_started",
	"notification_sent",
	"password_reset",
	"profile_updated",
	"search_query",
	"file_uploaded",
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Kafka broker addresses — configurable via environment variable.
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events-topic"
	}

	slog.Info("starting producer",
		"brokers", brokerList,
		"topic", topic,
	)

	// Configure the Kafka writer.
	// - BatchTimeout is kept low to minimise publish latency for this demo.
	// - Balancer uses LeastBytes to distribute messages evenly across partitions.
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false, // synchronous writes so we can log errors immediately
	}

	// Set up context cancellation on OS signals for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig.String())
		cancel()
	}()

	// Main production loop — fires every 500ms.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	messageCount := 0

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down producer", "messages_sent", messageCount)
			if err := writer.Close(); err != nil {
				slog.Error("failed to close kafka writer", "error", err)
			} else {
				slog.Info("kafka writer closed cleanly")
			}
			return

		case <-ticker.C:
			event := Event{
				ID:        generateID(),
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				Data:      mockData[rand.Intn(len(mockData))],
			}

			payload, err := json.Marshal(event)
			if err != nil {
				slog.Error("failed to marshal event", "error", err)
				continue
			}

			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(event.ID),
				Value: payload,
			})
			if err != nil {
				// Context cancellation is expected during shutdown.
				if ctx.Err() != nil {
					slog.Info("write aborted due to shutdown")
					continue
				}
				slog.Error("failed to write message", "error", err)
				continue
			}

			messageCount++
			slog.Info("message produced",
				"id", event.ID,
				"data", event.Data,
				"count", messageCount,
			)
		}
	}
}

// generateID produces a simple unique-enough identifier for demo purposes.
// Format: evt-<unix_nano>-<random_suffix>
func generateID() string {
	return fmt.Sprintf("evt-%d-%04x", time.Now().UnixNano(), rand.Intn(0xFFFF))
}
