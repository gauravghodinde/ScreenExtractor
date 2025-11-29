# Go multi-stage build for orchestrator
FROM golang:1.23-alpine AS builder

# Install git for go modules that need it
RUN apk add --no-cache git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build the orchestrator binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/local/bin/orchestrator ./cmd/orchestrator

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=builder /usr/local/bin/orchestrator /usr/local/bin/orchestrator
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orchestrator"]
