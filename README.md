# Flip Server Exercise

A Python service that uses [Temporal.io](https://temporal.io) to reliably flip strings via an unreliable HTTP server.

## Quick Start

```bash
# Install dependencies
poetry install

# Terminal 1 — Start local Temporal server
temporal server start-dev

# Terminal 2 — Start the flip server (black-box, do not modify)
poetry run python exercise/server.py

# Terminal 3 — Start the Temporal worker
poetry run python exercise/worker.py

# Terminal 4 — Run the client
poetry run python exercise/client.py "hello" "world"
```

## Requirements

1. Accept arbitrary number of strings to flip
2. Reliably flip all of them
3. Temporal `query` for number of strings flipped so far (progress)
4. Temporal `query` for number of `HTTP 200 OK` responses received (work tracking)

## API Server

A FastAPI wrapper exposes the Temporal workflow over HTTP:

```bash
# Start the API server
poetry run python exercise/api.py
```

**Endpoint:** `POST http://127.0.0.1:8080/flip`

```bash
curl http://127.0.0.1:8080/flip \
  -H "Content-Type: application/json" \
  -d '["hello", "world"]'
# => ["olleh", "dlrow"]
```

## Docker Compose

Run the entire stack with Docker Compose:

```bash
docker compose up --build
```

This starts four services:
- **temporal** — Temporal server (ports 7233, 8233)
- **flip-server** — The unreliable flip server (port 8000)
- **worker** — Temporal worker
- **api** — FastAPI wrapper (port 8080)

```bash
# Test the API
curl http://localhost:8080/flip \
  -H "Content-Type: application/json" \
  -d '["hello", "world"]'
```

## Kubernetes

Kubernetes manifests are in `k8s/`. They deploy the same four services into a `flip` namespace.

```bash
# Build the image (for minikube, run `eval $(minikube docker-env)` first)
docker build -t flip-exercise:latest .

# Deploy
kubectl apply -f k8s/

# Check status
kubectl -n flip get pods

# Port-forward the API for local access
kubectl -n flip port-forward svc/api 8080:8080
```

## Testing

```bash
# Run all tests
poetry run pytest

# Run specific test
poetry run pytest -k "test_name"

# Run with verbose output
poetry run pytest -v
```

## Linting & Formatting

```bash
# Format code
poetry run black .

# Sort imports
poetry run isort .

# Lint
poetry run pylint exercise/

# Type check
poetry run mypy exercise/
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server gRPC address |
| `FLIP_SERVER_URL` | `http://127.0.0.1:8000/flip` | Flip server endpoint (worker only) |

## Server Quirks

The flip server (`exercise/server.py`) is a black-box with these behaviors:

1. 10% chance of random 500 failure
2. 5-second processing delay per request
3. Only accepts lowercase a-z strings (else 400)
4. 10% per-string chance of returning `"-"` instead of the flip (partial failure on 200)
5. Max 10 strings per request (else 400)

## Architecture

- **`exercise/server.py`** — Black-box flip server (do not modify)
- **`exercise/workflows/workflow.py`** — Temporal workflow and activity definitions
- **`exercise/worker.py`** — Temporal worker process
- **`exercise/client.py`** — CLI client with query polling
- **`exercise/api.py`** — FastAPI wrapper around Temporal
