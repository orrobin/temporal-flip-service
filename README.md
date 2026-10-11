# Temporal Flip Service

Reliable processing on top of an unreliable HTTP service, built with durable [Temporal](https://temporal.io) workflows.

The downstream service (simulated in `exercise/server.py`) is deliberately hostile: it randomly returns HTTP 500, takes 5 seconds per request, accepts at most 10 strings per call, rejects anything that is not lowercase a to z, and sometimes returns HTTP 200 with a corrupted result for individual items. This service turns any number of input strings into a guaranteed, complete result anyway.

## How it works

- **Validation up front.** Invalid input fails fast as a non retryable error, so the workflow never loops on requests that can never succeed.
- **Batching and parallelism.** Pending strings are split into batches of 10 and sent concurrently as Temporal activities.
- **Retries with backoff.** Transport errors and HTTP 500s are retried by a Temporal retry policy with exponential backoff and a 30 second activity timeout.
- **Partial failure recovery.** A 200 response is not trusted blindly. Items that come back corrupted are tracked by their original index and sent again in the next round, until every string is flipped, with output order preserved.
- **Live progress.** Two Temporal queries expose progress while the workflow runs: strings flipped so far and successful HTTP responses received.
- **Durability.** Because state lives in the workflow, a worker crash or restart resumes the job instead of losing it.

## Stack

Python 3.11, Temporal, FastAPI, httpx, Docker Compose, Kubernetes, pytest (unit, workflow and load tests), black, isort, pylint, mypy

## Quick start

```bash
poetry install

# Terminal 1: local Temporal server
temporal server start-dev

# Terminal 2: the unreliable downstream service
poetry run python exercise/server.py

# Terminal 3: Temporal worker
poetry run python exercise/worker.py

# Terminal 4: run a job from the CLI (polls progress queries while it runs)
poetry run python exercise/client.py "hello" "world"
```

### HTTP API

```bash
poetry run python exercise/api.py

curl http://127.0.0.1:8080/flip \
  -H "Content-Type: application/json" \
  -d '["hello", "world"]'
# => ["olleh", "dlrow"]
```

### Docker Compose

```bash
docker compose up --build
```

Starts four services: `temporal` (7233, 8233), `flip-server` (8000), `worker`, and `api` (8080).

### Kubernetes

Manifests in `k8s/` deploy the same four services into a `flip` namespace.

```bash
docker build -t flip-exercise:latest .   # for minikube, run: eval $(minikube docker-env)
kubectl apply -f k8s/
kubectl -n flip get pods
kubectl -n flip port-forward svc/api 8080:8080
```

## Testing and code quality

```bash
poetry run pytest -v              # activity and workflow tests
poetry run python tests/load_test.py

poetry run black . && poetry run isort .
poetry run pylint exercise/
poetry run mypy exercise/
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server gRPC address |
| `FLIP_SERVER_URL` | `http://127.0.0.1:8000/flip` | Downstream service endpoint (worker only) |

## Project layout

| Path | Purpose |
|------|---------|
| `exercise/workflows/workflow.py` | Workflow, activity, retry policy and progress queries |
| `exercise/worker.py` | Temporal worker process |
| `exercise/client.py` | CLI client with live progress polling |
| `exercise/api.py` | FastAPI wrapper around the workflow |
| `exercise/server.py` | Simulated unreliable downstream service |
| `tests/` | Unit, workflow and load tests |
| `k8s/` | Kubernetes manifests |
