# Integrated Queueing System

Kotlin + Spring Boot WebFlux 기반 분산 대기열/참가열 관리 시스템. Redis Sorted Set으로 큐를 관리하고, SSE로 실시간 순위를 전달한다.

## 응답 언어 지침

- 모든 결과값, 설명, 주석, 커밋 메시지, PR 설명 등은 반드시 한글로 작성한다.
- 코드 내 변수명, 함수명, 클래스명 등 식별자는 영문을 유지하되, 그 외 사람이 읽는 텍스트는 한글로 작성한다.

## 프로젝트 개요

Docker Compose로 11개 서비스(앱 3개, Redis master/slave/sentinel, Nginx, Prometheus/Grafana)가 기동되는 분산 환경이다. 별도의 Reserve(예약) 프로젝트와 Docker Compose로 통합되어 티켓팅 시스템을 구성한다. 대기열 시스템이 트래픽을 제어하고, 참가열로 승격된 사용자가 예약 서비스로 이동한다.

## 빌드 및 실행

```bash
# 빌드
./gradlew build

# 테스트 실행
./gradlew test

# 단일 테스트 클래스 실행
./gradlew test --tests "com.example.integrated.ExceptionTest"

# Docker 이미지 빌드 및 인프라 기동
docker build -t ayeah77/integrated-queueing .
docker-compose up
```

## 기술 스택

- 프레임워크: Kotlin + Spring Boot WebFlux (비동기/논블로킹)
- 비동기: Kotlinx Coroutines + Project Reactor
- 큐 저장소: Redis Sentinel (master-slave, Redisson 분산 락)
- 실시간: SSE + Redis Pub/Sub
- 로드밸런서: Nginx (라운드로빈, 앱 3개 인스턴스)
- 모니터링: Prometheus + Grafana

## 아키텍처 핵심

트래픽 흐름: 사용자 요청 → Nginx → Lua 스크립트로 원자적 대기열/참가열 삽입 → Redis Pub/Sub → SSE 알림

Redis 키 구조:
- 대기열: `{queueType}:user-queue:wait` — Sorted Set, score = epoch-ms 20비트 시프트 + sequence
- 참가열: `{queueType}:user-queue:allow` — Sorted Set, score = 만료 타임스탬프
- 활성 큐 플래그: `active-allow-queue` — 스케줄링 활성 큐 타입 추적
- Pub/Sub 채널: `queueing_system`

분산 안전성:
- Redisson 분산 락(`scheduling_key`, 4초 lease)으로 스케줄러 단일 실행 보장
- Lua 스크립트(`enqueue-or-allow.lua`, `schedule-promote.lua`)로 Redis 연산 원자성 보장

## Gotchas

- CancellationException 재발생 필수 — 코루틴 `catch (e: Exception)` 블록에서 `CancellationException`은 반드시 `throw e`로 재발생시킨다. 삼키면 코루틴 취소가 전파되지 않아 리소스 누수 발생. `RedisLockUtil` 참고.
- Lua 스크립트 반환 값 연결 — `scripts/enqueue-or-allow.lua`의 반환 값(-1, -2, 0, 1)이 `QueueService.registerUserToWaitQueue()`의 `when` 분기와 직접 연결된다. 양쪽을 반드시 함께 수정한다.
- SSE 조회 순서 — `SseEventService.buildSseEvent()`는 대기열(ZRANK) → 참가열(ZSCORE) 순서로 조회한다. Lua 스크립트의 원자성으로 대기열에서 빠진 사용자는 반드시 참가열에 존재하므로 재확인이 불필요하다.
- Redisson 분산 락 leaseTime 4초 — 활성 큐 타입이 증가하면 4초 내에 모든 승격 처리가 완료되는지 확인해야 한다.
- 설정 값은 `application.properties` 직접 확인 — CLAUDE.md에 설정 값을 중복 기재하지 않는다. 불일치 방지를 위함.

## 코딩 컨벤션

- Loggable 인터페이스 — 모든 `@Service`/`@Component`/`@Controller`에 `: Loggable`을 구현한다. `log` 프로퍼티로 KotlinLogging 사용.
- Reactor→코루틴 브릿지 — 값 보장 시 `awaitSingle()`, null 가능 시 `awaitSingleOrNull()`, 빈 Flux 시 `awaitFirstOrNull()`.
- 병렬화 패턴 — 독립적인 suspend 호출은 `coroutineScope { list.map { async { ... } }.awaitAll() }` 패턴 사용.
- SSE 이벤트 추가 — `SseEvent` sealed class를 상속하고, `event` 프로퍼티에 SSE 이벤트명을 지정한다.
- 예외 처리 — `ReserveException(HttpStatus, ErrorCode)`로 발생시킨다. 새 에러 유형은 `ErrorCode` enum에 추가.

## Compact Instructions

컨텍스트 압축 시 다음을 반드시 보존한다:
- Gotchas 섹션 전체 (CancellationException, Lua 반환 값 규약)
- 코딩 컨벤션의 Loggable 인터페이스 규칙과 Reactor-코루틴 브릿지 패턴
- 응답 언어 지침 (한글)
