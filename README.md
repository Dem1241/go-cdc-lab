# Go CDC Lab

Small CDC playground using PostgreSQL, Debezium, Redpanda, and Go.

## Stack

- PostgreSQL
- Debezium
- Redpanda
- Go

## Flow

```text
PostgreSQL -> Debezium -> Redpanda -> Go
```

PostgreSQL changes are captured through logical replication, published to Redpanda, and consumed by the Go service.

## Run

```bash
docker compose up -d
```

Then start the consumer:

```bash
cd consumer
go run .
```

Changes made to `public.customers` in PostgreSQL will appear in the Go consumer.