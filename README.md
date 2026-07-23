# Flash Sale High-Concurrency System

A high-throughput, low-latency Flash Sale backend system built with **Spring Boot**, **Redis**, **Apache Kafka (KRaft mode)**, and **PostgreSQL**.

For full detailed architecture specifications, system design breakdown, Redis Lua mechanics, and Kafka event flow, refer to:
- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) (Full Architecture & Setup Guide)
- [KAFKA_EXPLAINED.md](KAFKA_EXPLAINED.md) (Easy Beginner-Friendly Kafka Guide)

## Core Architecture Highlights

1. **In-Memory Atomic Inventory Control (Redis + Lua)**: High-speed stock validation and deduction in memory (<2ms), eliminating race conditions and overselling without DB locks.
2. **Asynchronous Event Ingestion (Kafka Producer)**: Instant HTTP `202 ACCEPTED` responses. Requests are published to Kafka topic `flash-sale-orders` partitioned by `itemId`.
3. **Eventual Persistence (Kafka Consumer + PostgreSQL)**: Asynchronous background consumption that persists orders and updates canonical inventory in PostgreSQL under database transactions.

## Quick Start

### 1. Start Infrastructure Services
```bash
docker-compose up -d
```

### 2. Pre-Warm Inventory Stock in Redis
```bash
redis-cli SET item:stock:1 100
```

### 3. Run Application
```bash
./mvnw spring-boot:run
```

### 4. Place Flash Sale Order
```bash
curl -X POST http://localhost:8080/api/v1/orders/async \
  -H "Content-Type: application/json" \
  -d '{"userId": 101, "itemId": 1}'
```
