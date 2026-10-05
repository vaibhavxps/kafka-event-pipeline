#!/bin/bash
# ============================================================
#  Kafka Pipeline — GCP One-Click Deploy Script
#  
#  Run this in GCP Cloud Shell (https://console.cloud.google.com)
#  Click the >_ icon at the top-right to open Cloud Shell.
#
#  Usage: bash deploy_gcp.sh
# ============================================================

set -e  # Stop on any error

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Kafka Event Processing Pipeline — GCP Deployment       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Configuration ──
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
ZONE="us-central1-a"
CLUSTER_NAME="kafka-pipeline-cluster"

if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" == "(unset)" ]; then
    echo "❌ No GCP project set. Run:"
    echo "   gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo "📋 Project:  $PROJECT_ID"
echo "📍 Zone:     $ZONE"
echo "🔧 Cluster:  $CLUSTER_NAME"
echo ""
read -p "Press Enter to start deployment (Ctrl+C to cancel)..."

# ============================================================
# PHASE 1: Create all project files
# ============================================================
echo ""
echo "━━━ Phase 1/5: Creating project files ━━━"

mkdir -p ~/pipeline/producer ~/pipeline/consumer ~/pipeline/k8s

# ── Producer go.mod ──
cat > ~/pipeline/producer/go.mod << 'GOMOD'
module github.com/pipeline/producer

go 1.21

require github.com/segmentio/kafka-go v0.4.47

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
GOMOD

# ── Producer main.go ──
cat > ~/pipeline/producer/main.go << 'GOFILE'
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

type Event struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

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

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events-topic"
	}

	slog.Info("starting producer", "brokers", brokerList, "topic", topic)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig.String())
		cancel()
	}()

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
				if ctx.Err() != nil {
					slog.Info("write aborted due to shutdown")
					continue
				}
				slog.Error("failed to write message", "error", err)
				continue
			}

			messageCount++
			slog.Info("message produced", "id", event.ID, "data", event.Data, "count", messageCount)
		}
	}
}

func generateID() string {
	return fmt.Sprintf("evt-%d-%04x", time.Now().UnixNano(), rand.Intn(0xFFFF))
}
GOFILE

# ── Producer Dockerfile ──
cat > ~/pipeline/producer/Dockerfile << 'DOCKERFILE'
FROM golang:1.21-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /producer .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 1000 appuser
USER appuser
COPY --from=builder /producer /usr/local/bin/producer
ENTRYPOINT ["producer"]
DOCKERFILE

# ── Consumer go.mod ──
cat > ~/pipeline/consumer/go.mod << 'GOMOD'
module github.com/pipeline/consumer

go 1.21

require (
	github.com/prometheus/client_golang v1.19.0
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	google.golang.org/protobuf v1.32.0 // indirect
)
GOMOD

# ── Consumer main.go ──
cat > ~/pipeline/consumer/main.go << 'GOFILE'
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

type Event struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

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

	slog.Info("starting consumer", "brokers", brokerList, "topic", topic, "group_id", groupID, "metrics_addr", metricsAddr)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig.String())
		cancel()
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	server := &http.Server{Addr: metricsAddr, Handler: mux}

	go func() {
		slog.Info("metrics server starting", "addr", metricsAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
			cancel()
		}
	}()

	consumeLoop(ctx, reader)

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

func consumeLoop(ctx context.Context, reader *kafka.Reader) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("consume loop stopped (context cancelled)")
				return
			}
			slog.Error("error fetching message", "error", err)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal event", "error", err, "partition", msg.Partition, "offset", msg.Offset)
			if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
				slog.Error("failed to commit bad message", "error", commitErr)
			}
			continue
		}

		time.Sleep(200 * time.Millisecond)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("failed to commit message", "error", err, "id", event.ID)
			continue
		}

		messagesProcessed.Inc()

		slog.Info("message processed", "id", event.ID, "data", event.Data, "partition", msg.Partition, "offset", msg.Offset)
	}
}
GOFILE

# ── Consumer Dockerfile ──
cat > ~/pipeline/consumer/Dockerfile << 'DOCKERFILE'
FROM golang:1.21-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /consumer .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 1000 appuser
USER appuser
COPY --from=builder /consumer /usr/local/bin/consumer
EXPOSE 8080
ENTRYPOINT ["consumer"]
DOCKERFILE

# ── Kafka YAML ──
cat > ~/pipeline/k8s/kafka.yaml << 'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
  labels:
    app: zookeeper
spec:
  replicas: 1
  selector:
    matchLabels:
      app: zookeeper
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      containers:
        - name: zookeeper
          image: confluentinc/cp-zookeeper:7.5.0
          ports:
            - containerPort: 2181
          env:
            - name: ZOOKEEPER_CLIENT_PORT
              value: "2181"
            - name: ZOOKEEPER_TICK_TIME
              value: "2000"
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
spec:
  selector:
    app: zookeeper
  ports:
    - port: 2181
      targetPort: 2181
  clusterIP: None
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  labels:
    app: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: confluentinc/cp-kafka:7.5.0
          ports:
            - containerPort: 9092
          env:
            - name: KAFKA_BROKER_ID
              value: "1"
            - name: KAFKA_ZOOKEEPER_CONNECT
              value: "zookeeper:2181"
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "PLAINTEXT://kafka:9092"
            - name: KAFKA_LISTENERS
              value: "PLAINTEXT://0.0.0.0:9092"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
              value: "true"
          resources:
            requests:
              memory: "512Mi"
              cpu: "500m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  selector:
    app: kafka
  ports:
    - port: 9092
      targetPort: 9092
  clusterIP: None
YAML

# ── Producer Deployment YAML (image placeholder replaced later) ──
cat > ~/pipeline/k8s/producer-deployment.yaml << YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
  labels:
    app: producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: producer
  template:
    metadata:
      labels:
        app: producer
    spec:
      containers:
        - name: producer
          image: gcr.io/${PROJECT_ID}/producer:latest
          imagePullPolicy: Always
          env:
            - name: KAFKA_BROKERS
              value: "kafka:9092"
            - name: KAFKA_TOPIC
              value: "events-topic"
          resources:
            requests:
              memory: "64Mi"
              cpu: "100m"
            limits:
              memory: "128Mi"
              cpu: "250m"
      initContainers:
        - name: wait-for-kafka
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              echo "Waiting for Kafka to be ready..."
              until nc -z kafka 9092; do
                echo "Kafka not ready yet, retrying in 3s..."
                sleep 3
              done
              echo "Kafka is ready!"
YAML

# ── Consumer Deployment YAML ──
cat > ~/pipeline/k8s/consumer-deployment.yaml << YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer
  labels:
    app: consumer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: consumer
  template:
    metadata:
      labels:
        app: consumer
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: consumer
          image: gcr.io/${PROJECT_ID}/consumer:latest
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              name: metrics
              protocol: TCP
          env:
            - name: KAFKA_BROKERS
              value: "kafka:9092"
            - name: KAFKA_TOPIC
              value: "events-topic"
            - name: KAFKA_GROUP_ID
              value: "pipeline-consumer-group"
            - name: METRICS_ADDR
              value: ":8080"
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              memory: "64Mi"
              cpu: "100m"
            limits:
              memory: "128Mi"
              cpu: "250m"
      initContainers:
        - name: wait-for-kafka
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              echo "Waiting for Kafka to be ready..."
              until nc -z kafka 9092; do
                echo "Kafka not ready yet, retrying in 3s..."
                sleep 3
              done
              echo "Kafka is ready!"
---
apiVersion: v1
kind: Service
metadata:
  name: consumer
  labels:
    app: consumer
spec:
  selector:
    app: consumer
  ports:
    - port: 8080
      targetPort: 8080
      protocol: TCP
      name: metrics
  type: ClusterIP
YAML

# ── KEDA ScaledObject YAML ──
cat > ~/pipeline/k8s/keda-scaledobject.yaml << 'YAML'
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: consumer-scaledobject
  labels:
    app: consumer
spec:
  scaleTargetRef:
    name: consumer
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: "kafka:9092"
        consumerGroup: "pipeline-consumer-group"
        topic: "events-topic"
        lagThreshold: "10"
        offsetResetPolicy: "earliest"
YAML

echo "✅ All 10 project files created"

# ============================================================
# PHASE 2: Enable APIs & Configure Docker
# ============================================================
echo ""
echo "━━━ Phase 2/5: Enabling GCP APIs ━━━"

gcloud services enable container.googleapis.com --quiet
gcloud services enable containerregistry.googleapis.com --quiet

echo "✅ APIs enabled"

# ============================================================
# PHASE 3: Build & Push Docker Images
# ============================================================
echo ""
echo "━━━ Phase 3/5: Building & pushing Docker images ━━━"

gcloud auth configure-docker --quiet

# Build Producer
echo "🔨 Building producer image..."
cd ~/pipeline/producer
go mod tidy
docker build -t gcr.io/$PROJECT_ID/producer:latest .
echo "📤 Pushing producer image..."
docker push gcr.io/$PROJECT_ID/producer:latest

# Build Consumer
echo "🔨 Building consumer image..."
cd ~/pipeline/consumer
go mod tidy
docker build -t gcr.io/$PROJECT_ID/consumer:latest .
echo "📤 Pushing consumer image..."
docker push gcr.io/$PROJECT_ID/consumer:latest

echo "✅ Both images pushed to gcr.io/$PROJECT_ID/"

# ============================================================
# PHASE 4: Create GKE Cluster
# ============================================================
echo ""
echo "━━━ Phase 4/5: Creating GKE cluster (takes ~5 min) ━━━"

gcloud container clusters create $CLUSTER_NAME \
    --zone $ZONE \
    --num-nodes 3 \
    --machine-type e2-medium \
    --disk-size 30 \
    --quiet

gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE

echo "✅ GKE cluster '$CLUSTER_NAME' created"
kubectl get nodes

# ============================================================
# PHASE 5: Deploy everything to Kubernetes
# ============================================================
echo ""
echo "━━━ Phase 5/5: Deploying to Kubernetes ━━━"

cd ~/pipeline

# Deploy Kafka + Zookeeper
echo "📦 Deploying Kafka..."
kubectl apply -f k8s/kafka.yaml
echo "⏳ Waiting for Zookeeper..."
kubectl wait --for=condition=ready pod -l app=zookeeper --timeout=180s
echo "⏳ Waiting for Kafka..."
kubectl wait --for=condition=ready pod -l app=kafka --timeout=180s

# Deploy Producer
echo "📦 Deploying Producer..."
kubectl apply -f k8s/producer-deployment.yaml

# Deploy Consumer
echo "📦 Deploying Consumer..."
kubectl apply -f k8s/consumer-deployment.yaml
echo "⏳ Waiting for Consumer..."
kubectl wait --for=condition=ready pod -l app=consumer --timeout=180s

# Install KEDA
echo "📦 Installing KEDA..."
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.13.0/keda-2.13.0.yaml
echo "⏳ Waiting for KEDA (30s)..."
sleep 30
kubectl wait --for=condition=ready pod -l app.kubernetes.io/part-of=keda-operator -n keda --timeout=120s

# Apply ScaledObject
echo "📦 Applying KEDA ScaledObject..."
kubectl apply -f k8s/keda-scaledobject.yaml

# ============================================================
# DONE!
# ============================================================
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✅ DEPLOYMENT COMPLETE!                                ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║                                                          ║"
echo "║  Useful commands:                                        ║"
echo "║                                                          ║"
echo "║  kubectl get pods              ← see all pods            ║"
echo "║  kubectl logs -f deploy/producer --tail=10               ║"
echo "║  kubectl logs -f deploy/consumer --tail=10               ║"
echo "║  kubectl get scaledobject      ← KEDA status             ║"
echo "║  kubectl get hpa               ← autoscaler status       ║"
echo "║                                                          ║"
echo "║  Check metrics:                                          ║"
echo "║  kubectl port-forward svc/consumer 8080:8080             ║"
echo "║  Then click 'Web Preview' (port 8080) in Cloud Shell     ║"
echo "║  Add /metrics to the URL                                 ║"
echo "║                                                          ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  ⚠️  CLEANUP (run when done to avoid charges):           ║"
echo "║                                                          ║"
echo "║  gcloud container clusters delete $CLUSTER_NAME \\       ║"
echo "║      --zone $ZONE --quiet                                ║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

kubectl get pods
