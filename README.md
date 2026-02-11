# High-Throughput Fan-Out Engine

Java backend implementation of a distributed data fan-out and transformation engine, built to satisfy the assignment requirements for high-throughput streaming ingestion, parallel sink delivery, resiliency controls, and extensibility.

## 1. Project Summary

This service reads records from large flat files (CSV / JSONL / fixed-width), transforms each record for multiple downstream sink types, and dispatches them in parallel with per-sink throttling, retry, dead-letter handling, and status observability.

The implementation is mock-infrastructure based (as required), so it simulates:
- REST API sink behavior
- gRPC sink behavior
- message queue publishing behavior
- wide-column async UPSERT behavior

## 2. Assignment Requirement Coverage

### Functional Requirements

1. Concurrency
- Implemented using `ExecutorService` with support for Java 21 virtual threads.
- Sink workers run in parallel, each sink has an independent bounded queue and worker pool.

2. Config-driven runtime
- All runtime controls are externalized in YAML/JSON config:
  - input path and type
  - sink endpoints
  - rate limits
  - queue capacities
  - worker counts
  - retry/backoff settings
  - observability interval
  - DLQ path

3. Observability (every 5s by default)
- Status output includes:
  - records ingested
  - throughput (records/sec)
  - per-sink success/failure counts
  - accounted delivery totals

### Non-Functional Requirements

1. Zero data loss accounting
- Engine verifies: `expectedDeliveries == accountedDeliveries(success + failure)`.
- If mismatch occurs, run fails fast.

2. Scalability
- Default worker settings are CPU-aware and configurable.
- Backpressure is enforced via bounded queues to prevent unbounded memory growth.

3. Extensibility
- Strategy + plugin registry architecture allows adding new sink types without modifying core orchestrator logic.

### Evaluation-Rubric Alignment

1. Concurrency logic
- Uses `ExecutorService`, virtual threads, and async sink behavior (`CompletableFuture` in wide-column sink simulation).

2. Memory management / streaming
- File ingestion is streaming-based (line/record iteration), no full-file load into memory.
- Includes constrained-heap smoke run support (`-Xmx512m`) via rigorous test script.

3. Design patterns
- Strategy pattern for per-sink transformations.
- Factory for input-reader creation.
- Plugin registry for sink extensibility.

4. Resilience
- Per-sink token-bucket throttling.
- Retry with backoff.
- DLQ JSONL persistence after retry exhaustion.

5. Testing
- Unit tests for transformers and parsing/validation logic.
- Integration tests for orchestrator behavior using Mockito and edge-case runners.

## 3. Architecture

### Data Flow

1. Input reader streams source records.
2. Orchestrator fans each record out to all enabled sink pipelines.
3. Each sink pipeline:
- applies sink-specific transformation
- applies per-sink rate limiter
- executes send with retry/backoff
- records success/failure metrics
- writes failures to DLQ after retries exhausted
4. Periodic reporter prints status snapshots.
5. End-of-run accounting validates zero-loss requirement.

For a visual view, see:
- `/Users/apexcoder007/fanout_engine_project/docs/architecture.md`

## 4. Repository Layout

- `/Users/apexcoder007/fanout_engine_project/src/main/java`
  - application source
- `/Users/apexcoder007/fanout_engine_project/src/test/java`
  - unit and integration tests
- `/Users/apexcoder007/fanout_engine_project/config`
  - runnable config samples (CSV/JSONL/fixed-width)
- `/Users/apexcoder007/fanout_engine_project/samples`
  - sample input files
- `/Users/apexcoder007/fanout_engine_project/docs`
  - architecture notes
- `/Users/apexcoder007/fanout_engine_project/scripts`
  - automation scripts for repair and rigorous validation
- `/Users/apexcoder007/fanout_engine_project/pom.xml`
  - Maven build file

## 5. Prerequisites

- Java 21+
- Maven 3.9+

Recommended check:

```bash
java -version
mvn -version
```

## 6. Build and Run

### Build

```bash
cd /Users/apexcoder007/fanout_engine_project
mvn -s .mvn/settings.xml clean package
```

### Run with CSV config

```bash
java -jar target/high-throughput-fanout-engine-1.0.0.jar config/application.yaml
```

### Run with JSONL config

```bash
java -jar target/high-throughput-fanout-engine-1.0.0.jar config/application-jsonl.yaml
```

### Run with fixed-width config

```bash
java -jar target/high-throughput-fanout-engine-1.0.0.jar config/application-fixed-width.yaml
```

### Run tests

```bash
mvn -s .mvn/settings.xml clean test
```

## 7. Rigorous Validation Workflow

Use the comprehensive script:

```bash
cd /Users/apexcoder007/fanout_engine_project
./scripts/test-rigorous.sh
```

It performs:
1. full unit + integration test suite
2. shaded-jar package verification
3. end-to-end runs on CSV/JSONL/fixed-width configs
4. generated large-input run under constrained heap (`-Xmx512m`)

## 8. Configuration Reference

Main sample config:
- `/Users/apexcoder007/fanout_engine_project/config/application.yaml`

Additional samples:
- `/Users/apexcoder007/fanout_engine_project/config/application-jsonl.yaml`
- `/Users/apexcoder007/fanout_engine_project/config/application-fixed-width.yaml`

Key fields:

`input`
- `type`: `CSV` | `JSONL` | `FIXED_WIDTH`
- `path`: input file path
- `delimiter`: CSV delimiter
- `hasHeader`: CSV header toggle
- `fixedWidthFields`: field slicing definitions for fixed-width

`engine`
- `useVirtualThreads`
- `defaultWorkersPerSink`
- `queueCapacity`
- `maxRetries`
- `retryBackoffMillis`
- `shutdownTimeoutSeconds`
- `deadLetterPath`

`observability`
- `statusIntervalSeconds`

`sinks[]`
- `name`, `type`, `enabled`
- `endpoint`
- `rateLimitPerSecond`
- `workers`
- `queueCapacity`
- `minLatencyMs`, `maxLatencyMs`, `failureRate`
- optional sink-specific `options`

## 9. Sample Inputs Included

- `/Users/apexcoder007/fanout_engine_project/samples/customers.csv`
- `/Users/apexcoder007/fanout_engine_project/samples/customers.jsonl`
- `/Users/apexcoder007/fanout_engine_project/samples/customers_fixed_width.txt`

## 10. Resilience and Backpressure Details

- Backpressure: bounded `BlockingQueue` per sink pipeline.
- Throttling: token-bucket limiter per sink.
- Retry policy: up to configured max retries per record per sink.
- DLQ: failed records persisted as JSONL with timestamp, sink name, attempts, and error.

## 11. Observability Output

Status lines include:
- elapsed time
- records ingested
- throughput records/sec
- accounted deliveries
- per-sink success/failure counters

Run completion line:
- `[done] recordsIngested=... expectedDeliveries=... accountedDeliveries=...`

## 12. Testing Scope

Current suite covers:
- config type parsing and validation failures
- config loader format and missing-file handling
- CSV/JSONL/fixed-width ingestion behaviors and edge cases
- scalar parser edge values
- rate limiter guardrails and blocking behavior
- metrics snapshot and formatting
- sink-worker success/retry/failure/DLQ paths
- transformer correctness (JSON/Protobuf/XML/Avro)
- orchestrator integration and streaming edge cases

## 13. Troubleshooting

### Maven resolution/network issues

```bash
cd /Users/apexcoder007/fanout_engine_project
./scripts/repair-maven-resolution.sh
```

Project-local Maven config files:
- `/Users/apexcoder007/fanout_engine_project/.mvn/settings.xml`
- `/Users/apexcoder007/fanout_engine_project/.mvn/maven.config`
- `/Users/apexcoder007/fanout_engine_project/.mvn/jvm.config`

### If `rg` is missing

`test-rigorous.sh` now auto-falls back to `grep`; no manual install required.

### Protobuf `sun.misc.Unsafe` warnings

These are runtime warnings from protobuf on newer JDKs and are non-fatal for this project.

## 14. Assumptions

- Mock sinks simulate downstream behavior; no external infrastructure is required.
- Input record order across sinks is not guaranteed.
- Queue sizing and worker counts are tuned via config for deployment context.
- Extremely large single-record payloads may still require queue/heap tuning.
