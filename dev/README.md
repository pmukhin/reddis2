# Observability

Local monitoring stack for the Redis server: redis_exporter collects metrics, Prometheus stores them, and Grafana visualizes everything on a pre-configured dashboard. Use memtier_benchmark to generate load and watch the metrics in real time.

## Quick start

```bash
# 1. Build and start the Redis server
cargo build --release
./target/release/reddis2 &

# 2. Start the monitoring stack
cd dev
docker compose up -d

# 3. Generate load with memtier_benchmark
memtier_benchmark -s 127.0.0.1 -p 6379 --ratio=1:4 --test-time=60 -c 10 -t 4

# 4. Open Grafana and explore the metrics
open http://localhost:3000
# Login: admin / admin
```

## Endpoints

| Service          | URL                        |
|------------------|----------------------------|
| Grafana          | http://localhost:3000      |
| Prometheus       | http://localhost:9090      |
| Redis Exporter   | http://localhost:9121      |

## Teardown

```bash
docker compose down
```
