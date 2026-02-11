# Architecture

## Data Flow

```mermaid
flowchart LR
  A[Input Reader\nCSV / JSONL / Fixed-width] --> B[FanOut Orchestrator]
  B --> C1[Bounded Queue - REST]
  B --> C2[Bounded Queue - gRPC]
  B --> C3[Bounded Queue - MQ]
  B --> C4[Bounded Queue - Wide Column]
  C1 --> D1[Workers + Retry + RateLimiter]
  C2 --> D2[Workers + Retry + RateLimiter]
  C3 --> D3[Workers + Retry + RateLimiter]
  C4 --> D4[Workers + Retry + RateLimiter]
  D1 --> E1[REST Mock Sink\nJSON]
  D2 --> E2[gRPC Mock Sink\nProtobuf]
  D3 --> E3[MQ Mock Sink\nXML]
  D4 --> E4[Wide-Column Mock Sink\nAvro + CQL Map]
  D1 --> F[DLQ JSONL]
  D2 --> F
  D3 --> F
  D4 --> F
  B --> G[Metrics Reporter\nEvery 5 sec]
```

## Core Components

- `InputReaderFactory` builds streaming readers for CSV, JSONL, and fixed-width sources.
- `SinkPluginRegistry` maps sink types to strategy pairs (`Transformer` + `Sink`) so fan-out is extensible.
- `FanOutEngine` orchestrates ingestion, fan-out dispatch, worker lifecycle, and data-loss verification.
- `SinkWorker` enforces retry policy, backoff, throttling, and DLQ fallback.
- `TokenBucketRateLimiter` provides configurable per-sink rate limits.
- `MetricsCollector` tracks ingestion, throughput, and per-sink success/failure metrics.

## Extensibility Contract

To add a new sink without changing orchestrator logic:
1. Implement `SinkPlugin<T>`.
2. Register it with `SinkPluginRegistry`.
3. Add sink config entry in `application.yaml`.

No changes to `FanOutEngine` are required.
