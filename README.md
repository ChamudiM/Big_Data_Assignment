## Architecture

`order-producer` publishes Avro orders to `orders.raw`. `order-processor` validates them, publishes valid events to `orders.validated`, uses Kafka Streams to maintain category price averages, and routes permanent or exhausted failures to the dead-letter topic. The processor also serves the dashboard and operations API.

## Run with Docker in WSL

From this directory:

```bash
docker compose up --build -d
docker compose ps
```

Open the dashboard at <http://localhost:18083> and Kafka UI at <http://localhost:18080>. The producer API is on port `18082` and Schema Registry is on `18081`.

The automatic generator is enabled by default. For a controlled demo:

```bash
ORDER_GENERATOR_ENABLED=false docker compose up --build -d
```

## Operations API

- `GET /api/operations/summary` — counters, current averages, retry history, and DLQ entries
- `POST /api/operations/orders` — publish a demo order
- `POST /api/operations/dlq/{orderId}/replay` — replay a dead-letter event
- `GET /api/averages/{category}` — query the Kafka Streams state store
- `GET /actuator/health` — application health

## Verification

```bash
./mvnw clean verify
```
