# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 응답 언어 지침

- 모든 결과값, 설명, 주석, 커밋 메시지, PR 설명 등은 반드시 한글로 작성한다.
- 코드 내 변수명, 함수명, 클래스명 등 식별자는 영문을 유지하되, 그 외 사람이 읽는 텍스트는 한글로 작성한다.

## Project Overview

Integrated Queueing System — a distributed queue management backend built with Kotlin, Spring Boot WebFlux, and Kotlinx Coroutines. The system manages wait/allow queues using Redis Sorted Sets, processes events through Kafka, and delivers real-time updates via SSE.

## Build & Run Commands

```bash
# Build
./gradlew build

# Run tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.example.integrated.ExceptionTest"

# Build Docker image and start full infrastructure
docker build -t ayeah77/integrated-queueing .
docker-compose up
```

## Tech Stack

- **Language:** Kotlin 1.9.25, Java 17
- **Framework:** Spring Boot 3.5.4 with WebFlux (reactive, non-blocking)
- **Async:** Kotlinx Coroutines + Project Reactor
- **Database:** MySQL via R2DBC (async)
- **Cache/Queue Store:** Redis with Sentinel (master-slave replication, Redisson for distributed locks)
- **Messaging:** Apache Kafka (3-node KRaft cluster, 3 partitions, replication factor 3)
- **Real-time:** Server-Sent Events (SSE) with Redis Pub/Sub
- **Load Balancer:** Nginx (round-robin across 3 app instances)

## Architecture

### Request Flow

1. Client sends registration request with idempotency key (`request-key` header) → `QueueController`
2. `QueueService` validates, checks duplicates via `DuplicationCheckService` (R2DBC unique constraint)
3. `KafkaProducerService` publishes registration event to `queueing-system` topic
4. `KafkaConsumerService` consumes event, adds user to Redis wait queue (Sorted Set)
5. `QueueToAllowScheduler` (every 3s) promotes users from wait → allow queue using Redisson distributed lock
6. Redis Pub/Sub notifies all instances → SSE streams push rank/confirmation updates to clients

### Key Data Structures (Redis)

- **Wait Queue:** `{queueType}:user-queue:wait` — Sorted Set, score = epoch-ms shifted 20 bits + sequence
- **Allow Queue:** `{queueType}:user-queue:allow` — Sorted Set, score = expiration timestamp
- **Active Queue Flag:** `active-allow-queue` — tracks which queue types have active scheduling
- **Pub/Sub Channel:** `queueing_system`

### Module Layout

All source under `src/main/kotlin/com/example/integrated/`:

- `queue/queue/` — Core queue logic: controller, service, scheduler, DTOs
- `queue/kafka/` — Kafka producer/consumer and their configs
- `queue/duplication/` — Idempotency checking via R2DBC entity with unique constraint + 5-min TTL
- `queue/sse/` — SSE controller, service, and sealed event types (`UpdateSseEvent`, `ConfirmSseEvent`, `ErrorSseEvent`)
- `redis/` — Redis config (Sentinel), Redisson distributed lock utility, Pub/Sub publisher/listener
- `security/` — Spring Security WebFlux config with CORS (origin: `localhost:3000`)
- `reserveException/` — Custom `ReserveException`, `ErrorCode` enum, global exception handler
- `util/` — Constants (`WAIT_QUEUE`, `ALLOW_QUEUE`, `CHANNEL_NAME`), logging helpers

### Distributed Safety

- **Redisson lock** (`scheduling_key`, 4s lease) ensures only one instance runs the queue promotion scheduler at a time
- **Kafka idempotent producer** with `acks=all` and single in-flight request for ordering guarantees
- **`@RetryableTopic`** on consumer: 3 retries with 1s backoff, Dead Letter Topic fallback

### REST API Endpoints (all under `/queue`)

- `POST /register` — Register user to wait queue (requires `request-key` header)
- `GET /get/rank` — Get user's current queue rank
- `GET /create/cookie` — Issue HmacSHA256 access token as HTTP-only cookie (10-min TTL)
- `POST /isValidateToken/{token}` — Validate access token
- `POST /cancel` — Cancel user from queue
- `GET /stream` — SSE stream for real-time rank/confirmation updates

## Infrastructure (docker-compose)

- 3 app instances (ports 8081-8083) behind Nginx (port 8079)
- 3-node Kafka cluster (KRaft mode, ports 9092-9094)
- Redis master + 2 slaves + 3 Sentinels
- MySQL on host at port 3306

## Key Configuration (application.properties)

- `queue.allow.max-capacity=1000` — max users in allow queue
- `queue.allow.interval-ms=3000` — scheduler promotion interval
- `queue.validation.key=park` — queue validation key
- `queue.event.topic.name=queueing-system` — Kafka topic name
