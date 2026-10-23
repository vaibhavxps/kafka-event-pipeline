# Project Review II: Comprehensive Presentation & Reviewer Preparation Guide
**Project:** Distributed Real-Time Event Processing Pipeline  
**Candidate:** Vaibhav  
**Department:** Master of Computer Applications (MCA)  
**Project Guide:** Dr. Micheal Arok  
**Review Stage:** Second-Last Project Review (~65% Milestone)  
**Deliverable Files:** 
- [presentation.html](file:///c:/pipeline/presentation.html) (Interactive browser presentation with modern light theme & speaker notes toggle)
- [presentation_print.html](file:///c:/pipeline/presentation_print.html) (16:9 Landscape PDF export via `Ctrl + P`)

---

## 1. Slide-by-Slide Speaking Script
*Target Duration: 7 to 9 minutes (~45–60 seconds per slide).*

### Slide 1: Title Slide (Milestone II — 65% Progress)
> **Spoken Script:**  
> "Respected reviewers and my project guide Dr. Micheal Arok, good morning. Today I am presenting the Second-Last Project Review for the **Distributed Real-Time Event Processing Pipeline**.  
> The objective of this project is to build an elastic, cloud-native event processing system using Go, Apache Kafka, Kubernetes on Google Cloud, and KEDA for queue-lag autoscaling.  
> As this is our pre-final milestone review, the system is approximately 60 to 70 percent complete. The foundational streaming engine, container orchestration, and telemetry infrastructure are fully implemented and running on GKE. Today, I will walk you through our architectural design, completed milestones, empirical results, and the remaining roadmap before the final submission."

---

### Slide 2: Introduction & Problem Statement
> **Spoken Script:**  
> "To understand why this pipeline is needed, let us look at the problems in traditional web architectures:  
> 1. **Monolithic synchronous coupling**: When microservices communicate via REST or synchronous RPC, a bottleneck in any single service cascades backward, leading to request timeouts and total pipeline stalls.  
> 2. **The flaw of CPU-based autoscaling**: Standard Kubernetes Horizontal Pod Autoscalers (HPA) scale pods based on CPU and memory utilization. In streaming workloads, CPU spikes only occur *after* hundreds or thousands of unconsumed messages have already accumulated in the queue, violating strict SLAs.  
> 3. **Data loss risk during transient crashes**: In synchronous pipelines, worker pod crashes can drop in-flight requests.  
> 4. **Observability blindspots**: Without embedded telemetry, operators cannot diagnose backpressure or consumer lag until end-users report failures."

---

### Slide 3: Objectives & Scope of Work
> **Spoken Script:**  
> "To solve these challenges, we defined six concrete engineering objectives:  
> - **O1 (Event Producer):** A dedicated Go service that publishes structured JSON events at configurable rates with graceful OS signal handling.  
> - **O2 (Resilient Consumer Group):** A Go worker consumer utilizing consumer groups with at-least-once processing guarantees.  
> - **O3 (Lag-Triggered Autoscaling):** A KEDA `ScaledObject` that scales consumer pods up to 10 replicas directly when Kafka consumer group lag exceeds 10 messages.  
> - **O4 (Observability):** Native Prometheus metrics exposition on port 8080.  
> - **O5 (Containerization):** Multi-stage Alpine Docker images under 20MB running as an unprivileged non-root user.  
> - **O6 (Cloud Orchestration):** Production-ready Kubernetes manifests with liveness/readiness probes and TCP init containers running on a 3-node GKE cluster."

---

### Slide 4: Proposed Architecture & Data Flow
> **Spoken Script:**  
> "This slide illustrates our end-to-end data topology:  
> 1. The **Go Producer** constructs JSON events with UUIDs and UTC timestamps, pushing them to Kafka using a `LeastBytes` partition balancer and a 50ms batching window.  
> 2. The **Apache Kafka broker** persists events into `events-topic` as an immutable commit log, completely decoupling ingestion from processing.  
> 3. The **Go Consumer Group** continuously fetches messages, processes business payloads, commits offsets upon success, and increments our Prometheus counter.  
> 4. In parallel, the **KEDA controller** polls Kafka every 15 seconds. If unconsumed queue depth crosses our threshold of 10 messages, it proactively triggers the Kubernetes HPA to scale out consumer pods."

---

### Slide 5: Tools, Technologies & Engineering Methodology
> **Spoken Script:**  
> "Every technology was selected for a specific systems engineering reason:  
> - **Go (Golang 1.21):** Offers goroutine-level concurrency with minimal memory overhead—our pods consume only ~15MB RAM compared to 300MB+ for a typical Java/JVM service.  
> - **Apache Kafka 7.5.0:** Provides high-throughput durable message retention, partition rebalancing, and consumer offset tracking.  
> - **segmentio/kafka-go:** A pure Go client driver that avoids CGo bindings and `librdkafka`, allowing us to produce fully static binaries.  
> - **KEDA v2.13:** The CNCF event-driven autoscaler that makes queue depth a first-class autoscaling metric in Kubernetes.  
> - **Google Kubernetes Engine (GKE):** Provides production container orchestration, declarative rollouts, and self-healing pod restarts."

---

### Slide 6: Implementation & Work Completed
> **Spoken Script:**  
> "Here are the concrete components implemented in the codebase:  
> - In `producer/main.go`, we implemented an asynchronous worker with an OS signal channel (`SIGINT`, `SIGTERM`). On termination, the producer flushes buffered messages and closes the Kafka writer cleanly without dropping events.  
> - In `consumer/main.go`, we implemented `consumeLoop`, which deserializes JSON payloads and explicitly commits offsets only *after* successful processing. If a bad payload arrives, it is logged and committed forward to avoid pipeline deadlock.  
> - We also built a dual HTTP server hosting Prometheus `/metrics` and a Kubernetes `/healthz` probe.  
> - Finally, in `deploy_gcp.sh`, we scripted the full automated build, container push, GKE provisioning, and declarative deployment in a single executable workflow."

---

### Slide 7: Demonstration & Empirical Results
> **Spoken Script:**  
> "This slide presents empirical evidence captured from our live test deployment:  
> - On the top left, the **producer logs** demonstrate structured event publication (`user_signup`, `purchase_completed`, etc.) with sequential timestamps.  
> - Directly below, the **consumer logs** prove message acknowledgment, partition binding, and offset progression.  
> - On the right, querying `curl :8080/metrics` verifies that Prometheus successfully scrapes the `kafka_messages_processed_total` counter.  
> - The pod listing confirms that all pods—Zookeeper, Kafka, Producer, Consumer, and KEDA operator—run with zero restarts, while `kubectl get scaledobject` reports active status."

---

### Slide 8: Testing, Validation & Engineering Challenges
> **Spoken Script:**  
> "During our engineering iterations, we solved four critical real-world challenges:  
> 1. **Pod startup race conditions:** Kafka's JVM takes ~30 seconds to boot. App pods booted faster and crashed. We solved this by adding BusyBox init containers with `nc -z kafka 9092` loops.  
> 2. **Poison pill deadlock:** If a corrupted payload entered Kafka, unmarshal failures caused crash loops. We implemented error isolation so bad records are committed forward with error logs.  
> 3. **Autoscaler flapping:** Rapid scaling up and down causes pod churn. We stabilized this by calibrating a 60-second cooldown period.  
> 4. **Container hardening:** We eliminated root execution by defining a non-root user (`appuser`, UID 1000) and stripped binary symbols with `-ldflags="-s -w"`, cutting the container size down to just 16.8MB."

---

### Slide 9: Current Progress & Remaining Work (~65% Completed)
> **Spoken Script:**  
> "Here is our honest progress breakdown for this review:  
> - **Completed (65%):** The core Go producer, consumer group, Kafka integration, Prometheus telemetry, multi-stage Alpine Dockerfiles, Kubernetes manifests, KEDA ScaledObject, and GCP deployment automation are completely finished.  
> - **In Progress (15%):** Stress testing with high-volume message bursts (500 to 2,000 msg/s) to document empirical scale-out curves, and Grafana dashboard integration.  
> - **Remaining for Final Review (20%):** Implementing a Dead Letter Queue (DLQ) topic for routing corrupt messages, Avro/Protobuf schema registry validation, and a multi-broker high-availability Kafka setup.  
> This ensures that our foundational pipeline is solid before we layer on the final monitoring and resilience refinements."

---

### Slide 10: Conclusion & Next Steps
> **Spoken Script:**  
> "In conclusion, we have demonstrated a functional, cloud-native distributed streaming pipeline that replaces brittle synchronous calls and reactive CPU scaling with resilient Kafka buffering and proactive queue-lag autoscaling.  
> Our immediate priorities for the final review are completing our load benchmark graphs and integrating live Grafana dashboards.  
> Thank you for your time and guidance. I welcome your questions and feedback."

---

## 2. Reviewer Q&A: 10 Likely Questions & Answers

### Q1: Why did you scale based on Kafka lag instead of CPU or Memory?
> **Answer:**  
> "CPU and Memory metrics are lagging indicators in message streaming. In our testing, a single consumer pod can easily buffer thousands of incoming messages in network memory without immediately consuming 80% CPU. By the time the Kubernetes HPA registers a CPU spike and spins up new pods, end-to-end processing latency has already violated SLAs. In contrast, Kafka consumer lag measures the exact unconsumed backlog—if lag crosses 10 messages, KEDA triggers scaling immediately before backlog builds up."

### Q2: What delivery guarantees does your consumer provide, and how are duplicate messages handled?
> **Answer:**  
> "We provide **At-Least-Once** processing semantics. In `consumer/main.go`, we do not use automatic auto-commit. Instead, we use `FetchMessage` and only call `reader.CommitMessages()` *after* the payload has been successfully parsed and processed. If a pod crashes mid-execution, the uncommitted message will be redelivered to another consumer in the group. In the final phase, downstream idempotency will be enforced using unique event UUIDs stored in a deduplication cache (such as Redis)."

### Q3: How does Kafka partition rebalancing work when KEDA scales the consumer from 1 to 5 replicas?
> **Answer:**  
> "All consumer pods belong to the same consumer group: `pipeline-consumer-group`. When KEDA scales the Deployment from 1 to 5 replicas, each new pod registers with the Kafka group coordinator broker. Kafka triggers a group rebalance using its consumer group protocol. The topic's partitions are redistributed among the active pods. If the topic has 5 partitions, each consumer handles exactly 1 partition concurrently."

### Q4: What happens if a producer generates a corrupt or malformed JSON payload?
> **Answer:**  
> "In our implementation (`consumer/main.go`, lines 152–162), `json.Unmarshal` detects syntax errors. If deserialization fails, we log an error with the partition and offset, and explicitly commit the offset forward. This prevents a **poison pill** scenario where the consumer endlessly crashes and retries the same invalid record. In our remaining scope for the final review, we will route these rejected messages to a dedicated Dead Letter Queue (DLQ) topic."

### Q5: Why did you use `segmentio/kafka-go` rather than `confluent-kafka-go`?
> **Answer:**  
> "`confluent-kafka-go` is a CGo wrapper around the C library `librdkafka`. Compiling CGo requires a full C toolchain (GCC, musl/glibc), which significantly inflates Docker image build times and complicates creating minimal static binaries. In contrast, `segmentio/kafka-go` is written in 100% pure Go. It allows us to cross-compile a completely static binary (`CGO_ENABLED=0`) and package it into a minimal Alpine 3.19 runtime image measuring under 17MB."

### Q6: How do you handle cold-start ordering in Kubernetes when the cluster is booting up?
> **Answer:**  
> "Kafka takes significantly longer to initialize its JVM and connect to Zookeeper than a Go binary takes to launch. If our producer or consumer started immediately, their connection pool would throw connection refused errors and trigger Kubernetes CrashLoopBackOff. We solved this by adding a BusyBox init-container to both deployments that runs a lightweight TCP probe: `until nc -z kafka 9092; do sleep 3; done`. The application container only starts once port 9092 is actively listening."

### Q7: What is the significance of the 60-second cooldown period in your KEDA configuration?
> **Answer:**  
> "The cooldown period prevents **autoscaler thrashing** or flapping. When an event burst arrives, KEDA scales consumers up to meet demand. Once the backlog is drained, consumer lag instantly drops to zero. If the cooldown were too short (e.g., 5 seconds), Kubernetes would immediately terminate the pods, only to spin them back up when the next micro-burst hits. A 60-second cooldown ensures pods remain warm to handle near-term trailing traffic."

### Q8: How is observability implemented, and what metrics are exposed?
> **Answer:**  
> "In `consumer/main.go`, we initialized a Prometheus counter: `kafka_messages_processed_total`. We run a separate HTTP server on `:8080` using `promhttp.Handler()`. In our Kubernetes Deployment manifest, we added Prometheus scrape annotations (`prometheus.io/scrape: 'true'`). Any Prometheus server can scrape message throughput per pod. We also exposed `/healthz` for Kubernetes liveness and readiness probes."

### Q9: Why did you choose Google Kubernetes Engine (GKE) and what cluster configuration was used?
> **Answer:**  
> "We chose GKE because it provides managed master nodes, native VPC networking, and seamless Cloud Build/GCR integration. Our automated deployment script (`deploy_gcp.sh`) creates a 3-node cluster using `e2-medium` instances (2 vCPUs, 4GB RAM) in `us-central1-a`. This provides sufficient node capacity to schedule Zookeeper, Kafka, the KEDA operator, and multiple scaled consumer pods across distinct failure zones."

### Q10: What work remains between this review and the final project review?
> **Answer:**  
> "Approximately 35% of the project remains, focused on testing, resilience, and visualization:  
> 1. **High-throughput load testing:** Benchmarking the system under 500 to 2,000 events/sec to graph the exact scale-out response times and recovery curves.  
> 2. **Grafana dashboards:** Deploying Grafana with panels for message throughput, consumer lag, and consumer replica counts.  
> 3. **Dead Letter Queue (DLQ):** Creating an automated dead-letter routing topic for corrupted messages.  
> 4. **KRaft migration:** Exploring Kafka in KRaft mode to remove Zookeeper."

---

## 3. Pre-Review Preparation Checklist

### Terminal Windows to Pre-Stage:
- [ ] **Terminal 1 (Cluster Overview):**  
  `kubectl get pods -w`  
  *(Shows live status of Kafka, Zookeeper, Producer, Consumer, KEDA operator)*
- [ ] **Terminal 2 (Consumer Logs):**  
  `kubectl logs -f deploy/consumer --tail=15`  
  *(Shows live JSON processing logs with partition and offset)*
- [ ] **Terminal 3 (KEDA Status):**  
  `kubectl get scaledobject,hpa`  
  *(Shows lag threshold and active replica targets)*
- [ ] **Terminal 4 (Metrics Endpoint):**  
  `kubectl port-forward svc/consumer 8080:8080`  
  *(Enables instant access to http://localhost:8080/metrics)*

### Browser Tabs to Open:
- [ ] Tab 1: [presentation.html](file:///c:/pipeline/presentation.html) (Main slide deck)
- [ ] Tab 2: `http://localhost:8080/metrics` (Live Prometheus counter)
- [ ] Tab 3: Google Cloud Console / GKE Workloads page (if presenting live on GKE)

### Presentation Hotkeys:
- `Right Arrow` or `Space`: Next slide
- `Left Arrow`: Previous slide
- `N`: Toggle Speaker Notes drawer on/off
- `F`: Toggle Fullscreen mode
- `1` through `0`: Jump directly to Slide 1 through 10
