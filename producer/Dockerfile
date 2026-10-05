# ============================================================
# Stage 1: Build the Go binary
# ============================================================
FROM golang:1.21-alpine AS builder

# Install git (needed by go mod download for certain dependencies)
RUN apk add --no-cache git

WORKDIR /app

# Cache dependency downloads before copying full source
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Build a statically-linked binary with CGO disabled
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /producer .

# ============================================================
# Stage 2: Minimal runtime image
# ============================================================
FROM alpine:3.19

# Install CA certificates so the binary can make TLS connections
RUN apk add --no-cache ca-certificates

# Run as a non-root user for security
RUN adduser -D -u 1000 appuser
USER appuser

COPY --from=builder /producer /usr/local/bin/producer

ENTRYPOINT ["producer"]
