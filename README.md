# Cerberus: Fault-Tolerant Distributed Job Execution System

Cerberus is a background job execution system built from scratch with **Python, FastAPI, Redis Streams, PostgreSQL and Prometheus**. It was built to understand how reliable job-processing systems work internally, so queueing, retries, backoff, crash recovery, idempotency and dead-lettering are implemented directly instead of relying on Celery or BullMQ.

Jobs are submitted over a REST API, tracked durably in PostgreSQL, and distributed to multiple workers through Redis Streams consumer groups. If a worker crashes, its unacknowledged jobs are reclaimed by another worker. Failed jobs are retried with exponential backoff and jitter, and jobs that keep failing are moved to a Dead Letter Queue.

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
- [How Job Processing Works](#how-job-processing-works)
- [Job Lifecycle](#job-lifecycle)
- [Reliability Mechanisms](#reliability-mechanisms)
- [Redis Data Structures](#redis-data-structures)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Getting Started (Docker Compose)](#getting-started-docker-compose)
- [Running Without Docker](#running-without-docker)
- [Submitting Jobs](#submitting-jobs)
- [Demonstrating Failure Scenarios](#demonstrating-failure-scenarios)
- [Prometheus Metrics](#prometheus-metrics)
- [Running Tests](#running-tests)
- [Design Decisions](#design-decisions)
- [Known Limitations](#known-limitations)
- [Future Improvements](#future-improvements)
- [Learning Outcomes](#learning-outcomes)

## Key Features

- **REST API**: submit jobs with `POST /jobs` (FastAPI, with request validation and Swagger docs).
- **Redis Streams + consumer groups**: multiple independent workers share one queue.
- **Acknowledgements**: workers `XACK` a message only after processing finishes.
- **Crash recovery**: messages left pending by a dead worker are reclaimed with `XAUTOCLAIM` after an idle timeout.
- **Automatic retries**: failed jobs are scheduled in a Redis Sorted Set and re-queued by a retry scheduler when due.
- **Exponential backoff with jitter**: spreads retries out and avoids synchronized retry bursts.
- **Dead Letter Queue**: jobs that exhaust 5 attempts are moved to `cerberus:dlq` for inspection.
- **Idempotency checks**: `cerberus:processed:<job_id>` keys guard against re-processing redelivered jobs.
- **Durable job state**: PostgreSQL stores status, attempt count and timestamps for every job.
- **Observability**: Prometheus counters for submitted, completed, failed, retried and dead-lettered jobs at `/metrics`.
- **Containerized**: Docker Compose runs the API, worker, retry scheduler, Redis and PostgreSQL.

## Architecture

```text
                         Client
                           |
                           v
                    FastAPI REST API
                      POST /jobs
                           |
                 +---------+---------+
                 |                   |
                 v                   v
             PostgreSQL         Redis Stream
             Job records        cerberus:jobs
                                     |
                              Consumer Group
                              cerberus-workers
                                     |
                       +-------------+-------------+
                       |             |             |
                       v             v             v
                    Worker 1      Worker 2      Worker N
                       |             |             |
                       +-------------+-------------+
                                     |
                         +-----------+-----------+
                         |                       |
                         v                       v
                     Completed                Failed
                         |                       |
                        XACK              Retry scheduling
                                                 |
                                                 v
                                      Redis Sorted Set
                                       cerberus:retries
                                                 |
                                          Retry Scheduler
                                                 |
                                                 +----> Redis Stream

                         Permanently failing jobs
                                     |
                                     v
                              Redis Stream
                               cerberus:dlq

                 Prometheus metrics are exposed at /metrics
```

**Division of responsibility:** Redis handles execution and coordination (queueing, delivery, retries). PostgreSQL is the durable record of each job's state.

## How Job Processing Works

1. A client sends a job to `POST /jobs`.
2. FastAPI validates the request and generates a unique job ID.
3. The API stores the job record in PostgreSQL.
4. The API adds the job to the `cerberus:jobs` Redis Stream.
5. A worker in the `cerberus-workers` consumer group reads and processes the job.
6. On success, the job is marked `completed` and acknowledged with `XACK`.
7. On failure, the worker schedules a retry in `cerberus:retries`, subject to the maximum-attempt limit.
8. The retry scheduler finds due entries and adds them back to the main stream.
9. A job that reaches the maximum number of attempts is moved to `cerberus:dlq`.
10. If a worker leaves a message pending, another worker reclaims it with `XAUTOCLAIM` after the configured idle period.

## Job Lifecycle

```text
Success:            queued -> processing -> completed

Transient failure:  queued -> processing -> failed -> retry scheduled
                          -> processing -> completed

Permanent failure:  queued -> processing -> failed -> retry -> ... -> dead_letter
```

## Reliability Mechanisms

### Worker crash recovery
If a worker dies mid-job, it never sends `XACK`, so Redis keeps the message in the consumer group's Pending Entries List. Another worker detects messages that have been idle too long and takes ownership with `XAUTOCLAIM`, so the job is not lost.

### Retries, backoff and jitter
Failed jobs go into the `cerberus:retries` sorted set, scored by the time they become eligible. The retry scheduler polls this set and re-queues due jobs. Delays grow exponentially with each attempt, and a random jitter is added so many failing jobs do not all retry at the same instant.

### Maximum attempts and DLQ
A job is attempted at most **5 times**. After that, PostgreSQL is updated to `dead_letter`, the job is written to `cerberus:dlq`, the original message is acknowledged, and the dead-letter metric is incremented. This stops permanently failing jobs from retrying forever.

### Idempotency
Before processing, a worker checks `cerberus:processed:<job_id>`. If the key exists (for example, the job succeeded but the worker crashed before `XACK`), the job is acknowledged without being executed again.

> **Delivery guarantee:** Cerberus provides **at-least-once** delivery with idempotency checks to reduce duplicate execution. It does **not** claim exactly-once execution. See [Known Limitations](#known-limitations).

## Redis Data Structures

| Redis Key | Type | Purpose |
|---|---|---|
| `cerberus:jobs` | Stream | Main job queue |
| `cerberus:retries` | Sorted Set | Jobs waiting for their retry time |
| `cerberus:dlq` | Stream | Jobs that exceeded the retry limit |
| `cerberus:processed:<job_id>` | Key | Idempotency tracking |
| `cerberus-workers` | Consumer Group | Coordinates workers on the main stream |

## Technology Stack

| Technology | Role |
|---|---|
| Python | Application and worker implementation |
| FastAPI | REST API and request validation |
| Redis Streams | Main job queue and consumer groups |
| Redis Sorted Sets | Scheduled retry queue |
| PostgreSQL | Persistent job records and status |
| Prometheus Client | Application metrics |
| Docker Compose | Local multi-service environment |
| pytest | Automated testing |

## Repository Structure

```text
Cerberus/
├── app/
│   ├── main.py              # FastAPI app, POST /jobs, /metrics
│   ├── worker.py            # Consumer, processing, retries, DLQ, idempotency, XAUTOCLAIM
│   ├── retry_scheduler.py   # Moves due retries back to the main stream
│   ├── postgres_client.py   # Job persistence
│   ├── redis_client.py      # Redis connection setup
│   ├── metrics.py           # Prometheus metrics
│   └── models.py            # Request models
├── tests/
│   ├── test_main.py
│   ├── test_worker.py
│   └── test_retry_scheduler.py
├── .env                     # Create locally; do not commit
├── .gitignore
├── .dockerignore
├── Dockerfile
├── docker-compose.yml
├── init.sql
├── requirements.txt
└── README.md
```

## Getting Started (Docker Compose)

### Prerequisites

- Git
- Docker Desktop with Docker Compose
- Python 3.10+ (only needed to run tests locally)

### 1. Clone the repository

```bash
git clone https://github.com/shwetap3000/Cerberus.git
cd Cerberus
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://postgres:YOUR_POSTGRES_PASSWORD@localhost:5432/cerberus
POSTGRES_PASSWORD=YOUR_POSTGRES_PASSWORD
DB_HOST=localhost
REDIS_HOST=redis
```

Replace the placeholder with your own local password. **Never commit `.env` or real credentials.** Docker Compose overrides `DB_HOST` to `postgres` inside the containers; `localhost` is used when running outside Docker.

### 3. Build and start

```bash
docker compose up --build -d
```

| Service | Purpose |
|---|---|
| `api` | FastAPI application |
| `worker` | Job-processing worker |
| `retry-scheduler` | Requeues due retries |
| `redis` | Streams, consumer group, retry and idempotency data |
| `postgres` | Persistent job database |

```bash
docker compose ps                        # status
docker compose logs -f api               # also: worker, retry-scheduler
docker compose down                      # stop
docker compose down -v                   # stop AND delete Redis/PostgreSQL volumes
```

> **Warning:** `docker compose down -v` permanently deletes local job data.

To run more workers:

```bash
docker compose up -d --scale worker=3
```

## Running Without Docker

Requires Python 3.10+, a running Redis and a running PostgreSQL with the `jobs` table from `init.sql`.

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1            # Windows PowerShell
pip install -r requirements.txt

uvicorn app.main:app                  # terminal 1: API
python -m app.worker worker-1         # terminal 2: worker (start worker-2 etc. for more)
python -m app.retry_scheduler         # terminal 3: retry scheduler
```

## Submitting Jobs

Open the interactive docs at [http://localhost:8000/docs](http://localhost:8000/docs) and use `POST /jobs`:

```json
{
  "type": "normal",
  "payload": { "message": "Hello from Cerberus" }
}
```

Response:

```json
{
  "job_id": "generated-uuid",
  "stream_id": "generated-stream-id",
  "type": "normal",
  "payload": { "message": "Hello from Cerberus" },
  "status": "queued"
}
```

## Demonstrating Failure Scenarios

The worker includes job types that exercise each execution path:

| `type` | Behavior | What it demonstrates |
|---|---|---|
| `normal` | Completes successfully | Happy path, `XACK`, `completed` status |
| `fail_once` | Fails on attempt 1, succeeds afterward | Retry scheduling and backoff |
| `fail` | Fails every attempt | 5 attempts, then Dead Letter Queue |
| `slow` | Takes longer to run | Worker crash and pending-job recovery |

**Crash recovery demo:** start two workers, submit a `slow` job, kill the worker processing it, and watch the other worker reclaim the pending message via `XAUTOCLAIM` once the idle threshold passes.

## Prometheus Metrics

Metrics are exposed at [http://localhost:8000/metrics](http://localhost:8000/metrics).

| Metric | Description |
|---|---|
| `cerberus_jobs_submitted_total` | Jobs successfully submitted |
| `cerberus_jobs_completed_total` | Jobs completed successfully |
| `cerberus_jobs_failed_total` | Job processing failures |
| `cerberus_jobs_retried_total` | Retries scheduled |
| `cerberus_jobs_dead_lettered_total` | Jobs moved to the DLQ |

The API and workers are separate processes, and an in-memory Prometheus counter only exists inside the process that created it. To aggregate them, Cerberus uses the Prometheus client's multiprocess mode: `PROMETHEUS_MULTIPROC_DIR` points to a shared directory, and `/metrics` combines the data with a dedicated `CollectorRegistry` and `MultiProcessCollector`. When running locally without Docker, set the variable in every terminal before starting each process:

```powershell
mkdir .metrics
$env:PROMETHEUS_MULTIPROC_DIR="$PWD\.metrics"
```

## Running Tests

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install pytest httpx      # if not already in requirements.txt
python -m pytest -v
```

The suite contains **9 tests**:

| Area | What is checked | Count |
|---|---|---:|
| API | Valid job submission, invalid request validation | 2 |
| Worker | Successful processing, idempotency, retry scheduling, DLQ handling, pending-job recovery call | 5 |
| Retry Scheduler | Fetching due retries, moving a retry back to the main stream | 2 |
| **Total** | | **9** |

**Testing scope:** these tests mock Redis and PostgreSQL and verify application logic. They do not prove end-to-end behavior against live services. Failure scenarios (retry, DLQ, worker crash recovery) were exercised manually using Swagger, worker logs, Redis and Prometheus metrics.

## Design Decisions

- **Why Redis Streams?** They provide consumer groups, acknowledgements, pending-entry tracking and `XAUTOCLAIM`, which are the primitives needed to build a reliable queue directly.
- **Why PostgreSQL alongside Redis?** Redis coordinates execution; PostgreSQL keeps durable, queryable job state (status, attempts, timestamps).
- **Why not Celery or BullMQ?** The goal was to understand and implement queueing, acknowledgement, retries, backoff, idempotency and recovery, which a framework would hide.
- **Why a DLQ?** It isolates permanently failing jobs so they stop consuming retries and can be inspected separately.
- **Why no worker heartbeat?** Redis already tracks pending entries, so Cerberus recovers at the job level with `XAUTOCLAIM` instead of building a separate worker-liveness system.
- **Why no frontend dashboard?** Metrics are exposed via `/metrics`; a dashboard was kept out of scope to stay focused on the backend.

## Known Limitations

Cerberus is a portfolio and learning project, not a production-hardened job platform.

- **No end-to-end persistence verification:** the API-to-PostgreSQL path still needs a conclusive integration check against the running Docker services.
- **Not exactly-once:** Redis Streams plus idempotency keys reduce duplicate execution, but delivery is at-least-once.
- **Non-atomic failure windows:** a crash between separate Redis operations (for example retry scheduling and acknowledgement) can still create edge cases.
- **No authentication or authorization** on the API.
- **No bundled Prometheus server or Grafana dashboards**; only the `/metrics` endpoint is provided.
- **Local/demo deployment only**; not configured for production.

## Future Improvements

- Integration tests against real Redis and PostgreSQL.
- Atomic retry scheduling and acknowledgement (for example Lua scripts or transactions).
- Authentication and authorization.
- Prometheus server and Grafana dashboards.
- Job cancellation, priorities and scheduled jobs.
- Further operational safeguards for production deployment.

## Learning Outcomes

- Redis Streams, consumer groups, pending entries, `XACK` and `XAUTOCLAIM`
- Retry scheduling with Sorted Sets, exponential backoff and jitter
- Idempotency, duplicate-delivery handling and at-least-once semantics
- Dead Letter Queues and failure-path design
- FastAPI request validation and REST API development
- PostgreSQL persistence for durable job state
- Prometheus metrics, including multiprocess collection
- Docker Compose multi-service environments
- Testing with pytest and mocks

## Author

**[shwetap3000](https://github.com/shwetap3000)**: [github.com/shwetap3000/Cerberus](https://github.com/shwetap3000/Cerberus)
