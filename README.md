# Form Submission Pipeline

A production-quality implementation of a form submission API with real-time stream aggregation, built with **Laravel 11 + PHP 8.3**, **MySQL 8**, **Apache Kafka**, and **Apache Flink** — all running locally via Docker Compose.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Project Structure](#project-structure)
3. [Quick Start](#quick-start)
4. [Part 1 — REST API](#part-1--rest-api)
5. [Part 2 — Load Test](#part-2--load-test)
6. [Part 3 — Flink Aggregation](#part-3--flink-aggregation)
7. [Part 4 — Postman Collection](#part-4--postman-collection)
8. [Running Tests](#running-tests)
9. [MySQL Tuning Tradeoffs](#mysql-tuning-tradeoffs)
10. [Interview Talking Points](#interview-talking-points)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Docker Compose Network                          │
│                                                                         │
│  Client (k6 / curl / Postman)                                           │
│        │  HTTP POST /api/submissions                                    │
│        ▼                                                                │
│  ┌─────────────────────────────────┐                                   │
│  │  Laravel Octane (RoadRunner)    │  ← 8 persistent workers           │
│  │  app:8000                       │    (no cold-start per request)     │
│  └──────────────┬──────────────────┘                                   │
│                 │ DB transaction                                        │
│                 ▼                                                       │
│  ┌─────────────────────────────────┐                                   │
│  │  MySQL 8 (InnoDB, utf8mb4)      │                                   │
│  │  • submissions (UNIQUE key)     │                                   │
│  │  • outbox (transactional relay) │                                   │
│  │  • form_hourly_counts (sink)    │                                   │
│  └──────────────┬──────────────────┘                                   │
│                 │ poll (every 2 s)                                      │
│                 ▼                                                       │
│  ┌─────────────────────────────────┐                                   │
│  │  Outbox Worker (artisan)        │                                   │
│  └──────────────┬──────────────────┘                                   │
│                 │ publish                                               │
│                 ▼                                                       │
│  ┌─────────────────────────────────┐                                   │
│  │  Kafka (topic: submissions)     │                                   │
│  └──────────────┬──────────────────┘                                   │
│                 │ consume                                               │
│                 ▼                                                       │
│  ┌─────────────────────────────────┐                                   │
│  │  Apache Flink                   │                                   │
│  │  • Event-time watermarks (1min) │                                   │
│  │  • Tumbling 1-hour windows      │                                   │
│  │  • Early fire every 10 s        │                                   │
│  │  • EXACTLY_ONCE checkpointing   │                                   │
│  └──────────────┬──────────────────┘                                   │
│                 │ UPSERT                                                │
│                 ▼                                                       │
│  ┌─────────────────────────────────┐                                   │
│  │  MySQL form_hourly_counts       │                                   │
│  │  (ON DUPLICATE KEY UPDATE)      │                                   │
│  └─────────────────────────────────┘                                   │
│        │  GET /api/forms/{id}/stats                                    │
│        ▼                                                                │
│  Client (Postman / dashboard)                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
form_submit/
├── app/
│   ├── Console/Commands/
│   │   └── OutboxPublishCommand.php   # Outbox → Kafka worker
│   ├── Http/
│   │   ├── Controllers/Api/
│   │   │   ├── SubmissionController.php
│   │   │   └── StatsController.php
│   │   ├── Requests/
│   │   │   └── StoreSubmissionRequest.php  # HTTP 400 override
│   │   └── Resources/
│   │       ├── SubmissionResource.php
│   │       └── HourlyStatResource.php
│   ├── Models/
│   │   ├── Submission.php
│   │   ├── Outbox.php
│   │   └── FormHourlyCount.php
│   └── Services/
│       └── SubmissionService.php      # Business logic (INSERT IGNORE)
├── database/migrations/
│   ├── ..._create_submissions_table.php
│   ├── ..._create_outbox_table.php
│   └── ..._create_form_hourly_counts_table.php
├── docker/
│   ├── app/Dockerfile                 # PHP 8.3 + rdkafka + RoadRunner
│   ├── flink/Dockerfile               # Flink + JDBC + Kafka connector JARs
│   └── mysql/
│       ├── my.cnf                     # Tuned MySQL config
│       └── init/01-init.sql           # Creates app + app_testing databases
├── flink-job/
│   ├── pom.xml                        # Maven with MySQL JDBC driver
│   ├── src/main/java/com/formsubmit/
│   │   └── SubmissionAggregatorJob.java
│   └── flink-demo.sh                  # Fault tolerance demo
├── load-test/
│   ├── k6.js                          # 100k request load test
│   └── verify.sh                      # SQL verification queries
├── postman/
│   ├── Submission-API.postman_collection.json
│   └── Submission-API.local.postman_environment.json
├── routes/api.php
├── phpunit.xml                        # MySQL testing config
├── docker-compose.yml
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker + Docker Compose v2
- (Optional for load test) [k6](https://k6.io/docs/getting-started/installation/)
- (Optional for Flink build) Java 11 + Maven 3.8+

### 1. Clone and configure

```bash
cp .env.example .env
# The APP_KEY is already generated. Edit .env if needed.
```

### 2. Start all services

```bash
docker compose up -d --build
```

This starts: MySQL 8, Zookeeper, Kafka, Flink JM+TM, Laravel Octane, Outbox worker.

### 3. Wait for health checks

```bash
docker compose ps   # all services should show "healthy" or "running"
```

### 4. Verify the API

```bash
curl -s -X POST http://localhost:8000/api/submissions \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"form_id":"contact-form","idempotency_key":"test-1","email":"hi@example.com","data":{"name":"Test"}}' \
  | python3 -m json.tool
```

---

## Part 1 — REST API

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/submissions` | Create/idempotent-find submission |
| GET | `/api/forms/{form_id}/submissions` | List submissions (paginated) |
| GET | `/api/forms/{form_id}/stats` | Hourly counts from Flink |

### Test the API manually

```bash
# 1. Create (201)
curl -s -X POST http://localhost:8000/api/submissions \
  -H "Content-Type: application/json" \
  -d '{"form_id":"contact-form","idempotency_key":"abc-123","email":"user@example.com","data":{"name":"John"}}'

# 2. Duplicate (200 + duplicate:true + same id)
curl -s -X POST http://localhost:8000/api/submissions \
  -H "Content-Type: application/json" \
  -d '{"form_id":"contact-form","idempotency_key":"abc-123","email":"user@example.com","data":{"name":"John"}}'

# 3. Validation error (400)
curl -s -X POST http://localhost:8000/api/submissions \
  -H "Content-Type: application/json" \
  -d '{"form_id":"x","idempotency_key":"y","email":"bad","data":{}}'

# 4. List
curl -s http://localhost:8000/api/forms/contact-form/submissions
```

---

## Part 2 — Load Test

### Run with k6

```bash
# Using Docker (no local k6 needed):
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8000 \
  grafana/k6 run - < load-test/k6.js

# Or locally:
k6 run -e BASE_URL=http://localhost:8000 load-test/k6.js
```

### Expected results

| Metric | Expected |
|--------|----------|
| 201 Created | ~80,000 |
| 200 Duplicate | ~20,000 |
| Errors (non-2xx) | 0 |
| Throughput | ~1,000–3,000 req/s |
| p95 latency | < 100 ms |

### Verify with SQL

```bash
# Via Docker:
docker compose exec mysql mysql -uapp_user -psecret app \
  -e "SELECT COUNT(*) FROM submissions;"

# No duplicate keys:
docker compose exec mysql mysql -uapp_user -psecret app \
  -e "SELECT idempotency_key, COUNT(*) c FROM submissions GROUP BY idempotency_key HAVING c > 1;"

# Or use the helper script:
bash load-test/verify.sh
```

### Throughput tuning tips

| Lever | Setting | Effect |
|-------|---------|--------|
| Octane workers | `OCTANE_WORKERS=16` | More concurrent requests |
| MySQL buffer pool | `innodb_buffer_pool_size=1G` | Fewer disk reads |
| MySQL flush | `innodb_flush_log_at_trx_commit=2` | ~5-10× write throughput |
| k6 VUs | `--vus 100` | Higher concurrency |
| MySQL connections | Ensure `max_connections > workers + headroom` | Avoid "too many connections" |

---

## Part 3 — Flink Aggregation

### Build the Flink job

```bash
cd flink-job
mvn clean package -q
# Produces: target/flink-submission-aggregator-1.0-SNAPSHOT.jar
```

### Submit the job

```bash
# Via Flink REST API (job is submitted to the running cluster):
curl -X POST http://localhost:8081/jars/upload \
  -H "Expect:" \
  -F "jarfile=@flink-job/target/flink-submission-aggregator-1.0-SNAPSHOT.jar"

# Then list uploaded jars and get the jar id:
JAR_ID=$(curl -s http://localhost:8081/jars | python3 -c "import sys,json; print(json.load(sys.stdin)['files'][0]['id'])")

# Submit with MySQL + Kafka env vars:
curl -X POST "http://localhost:8081/jars/${JAR_ID}/run" \
  -H "Content-Type: application/json" \
  -d '{
    "programArgsList": [],
    "entryClass": "com.formsubmit.SubmissionAggregatorJob",
    "parallelism": 1,
    "flinkConfiguration": {
      "pipeline.object-reuse": "true"
    }
  }'

# Or use the Flink Web UI at http://localhost:8081
```

### Watch live counts

```bash
# Push a test submission:
curl -s -X POST http://localhost:8000/api/submissions \
  -H "Content-Type: application/json" \
  -d '{"form_id":"contact-form","idempotency_key":"live-1","email":"test@example.com","data":{"name":"Live"}}'

# Wait 10-15 seconds (early trigger fires), then query:
curl -s http://localhost:8000/api/forms/contact-form/stats | python3 -m json.tool

# Watch it grow in a loop:
watch -n 5 "curl -s http://localhost:8000/api/forms/contact-form/stats | python3 -m json.tool"
```

### Fault tolerance demo

```bash
chmod +x flink-job/flink-demo.sh
./flink-job/flink-demo.sh
```

---

## Part 4 — Postman Collection

### Import into Postman

1. Open Postman → Import → select both files:
   - `postman/Submission-API.postman_collection.json`
   - `postman/Submission-API.local.postman_environment.json`
2. Select the "Submission API — Local" environment
3. Run via Collection Runner

### Run with newman (CLI)

```bash
# Install newman:
npm install -g newman

# Run:
newman run postman/Submission-API.postman_collection.json \
  --environment postman/Submission-API.local.postman_environment.json \
  --reporters cli,json \
  --reporter-json-export load-test/newman-results.json

# Docker alternative (no local newman needed):
docker run --rm --network host \
  -v $(pwd)/postman:/etc/newman \
  postman/newman run Submission-API.postman_collection.json \
  --environment Submission-API.local.postman_environment.json
```

---

## Running Tests

> **Requires MySQL** running locally or via Docker (`docker compose up -d mysql`)

```bash
# Run all Pest tests against MySQL app_testing database:
php artisan test

# Or:
./vendor/bin/pest --colors=always
```

Test coverage:
- ✅ Validation → 400 (email, form_id, idempotency_key, data)
- ✅ Create → 201 with correct shape
- ✅ Duplicate → 200 + `duplicate:true` + same `id`
- ✅ Pagination → max 10 per page, links present

---

## MySQL Tuning Tradeoffs

| Setting | Value | Tradeoff |
|---------|-------|----------|
| `innodb_buffer_pool_size` | 512M | Caches data+index pages → fewer disk I/Os. Increase to 70% of RAM in production. If set too high, OS starts swapping. |
| `innodb_flush_log_at_trx_commit` | 2 | Writes log buffer to OS file cache on every commit, flushes to disk ~1/s. Risk: up to 1 second of data loss on hard crash. Gain: 5–10× write throughput. Use `1` (default) for financial/critical data. |
| `sync_binlog` | 0 | Disables binary log sync — best throughput for dev/load test. In production with replication, set to `1`. |
| `max_connections` | 200 | Must exceed (Octane workers) + (Flink pool) + (CLI/admin). Each idle connection costs ~1 MB RAM. |
| `skip-log-bin` | — | Disables binary log entirely — removes the overhead of writing the binlog per transaction. Do NOT use in production if you need replication or point-in-time recovery. |

---

## Interview Talking Points

### 1. Idempotency approach
We rely on MySQL's `UNIQUE` constraint on `idempotency_key` + `INSERT IGNORE` (Laravel's `insertOrIgnore()`). If two identical requests arrive simultaneously, MySQL serializes them at the storage layer — one INSERT wins, the other silently no-ops (errno 1062). We then `SELECT` the row and return it. This avoids the TOCTOU race condition of a check-then-insert pattern.

### 2. Race conditions
The classic race condition is: Thread A and Thread B both check "does this key exist?" → both see "no" → both INSERT → duplicate row. Our approach eliminates the check entirely. The UNIQUE constraint IS the check, and `INSERT IGNORE` makes the failure silent. This is lock-free from the application side.

### 3. Why the Outbox Pattern?
Publishing to Kafka inside the HTTP transaction would create a dual-write problem: if Kafka is down, should we roll back the DB insert? That would lose the submission. Should we commit and drop the event? That means data inconsistency. The outbox table, written in the same transaction as the submission, means the event row either commits with the submission or rolls back with it. The separate worker handles the Kafka publish asynchronously with retries.

### 4. Why Flink event time + watermarks?
Event time (the `created_at` in the Kafka message) is what matters for "submissions per hour" — not when Flink received the message. A message delayed in Kafka by 30 seconds should still be counted in its original hour. Watermarks tell Flink "I'm confident I've seen all events up to this point" so it can safely close a window. The 60-second slack tolerates minor reordering and network delays.

### 5. Checkpointing / exactly-once semantics
Flink snapshots: (a) Kafka consumer offsets, (b) window state (partial counts), and (c) any operator state — atomically, every 10 seconds. On recovery from a crash, Flink restores to the last successful checkpoint and replays from the saved Kafka offset. Double-counting is prevented because the MySQL sink uses `ON DUPLICATE KEY UPDATE` — replayed results overwrite, not add. This gives end-to-end exactly-once processing.

### 6. Why `UNIQUE` + `INSERT IGNORE` vs. `UPSERT`?
`INSERT IGNORE` on a new record is equivalent to a normal INSERT (O(1) b-tree insert). On a duplicate it does nothing — no update, no row lock held beyond the uniqueness check. A full `INSERT ... ON DUPLICATE KEY UPDATE` would lock and write even for duplicates, costing more I/O. For submissions we want true insert-or-skip semantics, not insert-or-update, so `INSERT IGNORE` is correct.

### 7. Scaling considerations
- **Horizontal API scaling**: Add more Octane containers behind a load balancer. Each has its own connection pool. MySQL `max_connections` must be raised proportionally.
- **Kafka partitions**: For higher throughput, increase partitions on the `submissions` topic and set Flink parallelism > 1. Each partition maps to one Flink source subtask.
- **MySQL**: For > 10M submissions, consider partitioning the `submissions` table by `form_id` hash, or archiving old rows. Read replicas for the listing endpoint.
- **Outbox worker**: Currently single-threaded. For very high throughput, shard by `id % N` into N parallel workers, or use Debezium CDC to stream the outbox changes directly into Kafka without polling.

### 8. Octane vs. PHP-FPM
PHP-FPM bootstraps the entire Laravel framework on every request (~5–20 ms cold start). Octane keeps the framework warm in memory — each request is just the route dispatch and business logic. At 100k requests, this saves ~500 seconds of bootstrap time. RoadRunner is chosen over Swoole because it's a pure Go binary (no PECL extension to install), and it handles HTTP/1.1 gracefully with minimal configuration.

### 9. simplePaginate vs. paginate
`paginate()` runs `SELECT COUNT(*) FROM submissions WHERE form_id = ?` before the data query. On a table with millions of rows this is expensive even with indexes (InnoDB must read all index entries for the count). `simplePaginate()` skips the count and only fetches `LIMIT n+1` rows to determine if there's a next page. The tradeoff: no total count in the response, and clients can't jump to page N directly. For an activity feed this is acceptable; for a back-office UI needing "page 47 of 500", use `paginate()` with a Redis-cached count.
