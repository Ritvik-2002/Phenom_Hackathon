# Phenom Hackathon

A multi-tenant event ingestion service backed by Kafka. The collector accepts authenticated HTTP events, enriches them with server-generated metadata, and publishes them to Kafka. A consumer writes each event as a JSON line grouped by tenant and date.

## Prerequisites

- Node.js 18 or later
- Docker and Docker Compose

## Run locally

```bash
npm install
docker compose up -d
```

In separate terminals, start the consumer and collector:

```bash
npm run consumer
npm run collector
```

The collector listens on `http://localhost:3000`. It creates the `user-events` Kafka topic automatically.

## Send an event

```bash
curl -X POST http://localhost:3000/events \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: key-123' \
  -d '{
    "event_type": "page_view",
    "user_id": "user_42",
    "page": "/pricing"
  }'
```

Valid development API keys are:

| API key | Tenant |
| --- | --- |
| `key-123` | `tenant_123` |
| `key-456` | `tenant_456` |

Each event must include a string `event_type`. The collector adds `event_id`, `tenant_id`, and `event_time`; client-provided values for those fields are ignored.

## Event output

The consumer writes newline-delimited JSON files to:

```text
data/events/<tenant>/<YYYY-MM-DD>.ndjson
```

For example, events for `tenant_123` are saved under `data/events/tenant_123/`.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `3000` | Collector HTTP port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | `user-events` | Kafka topic name |
| `KAFKA_PARTITIONS` | `4` | Partitions used when creating the topic |
| `OUTPUT_DIR` | `data/events` | Consumer output directory |
| `GROUP_ID` | `file-writer` | Kafka consumer group ID |
