# ============================================================
# Stage 1: Build the Go binary
# ============================================================
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /consumer .

# ============================================================
# Stage 2: Minimal runtime image
# ============================================================
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

RUN adduser -D -u 1000 appuser
USER appuser

COPY --from=builder /consumer /usr/local/bin/consumer

EXPOSE 8080

ENTRYPOINT ["consumer"]
