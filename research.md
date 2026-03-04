# 분산 대기열 시스템

## 1. 시스템 개요

본 시스템은 대규모 동시 접속 환경에서 사용자를 순서대로 관리하기 위한 분산 대기열 관리 백엔드 시스템입니다. 
티켓팅, 한정판 판매, 서버 입장 제한 등의 시나리오에서 사용자를 대기열(Wait Queue)에 순서대로 넣고, 참가열(Allow Queue)로 승격시키는 구조입니다.

### 핵심 설계 철학
- 비동기/논블로킹 : Spring WebFlux + Kotlin 코루틴 기반으로 높은 동시성 처리
- 분산 안정성 : Redisson 분산 락 + Kafka 멱등 프로듀서로 다중 인스턴스 환경에서의 일관성 보장
- 실시간 피드백 : Redis Pub/Sub + SSE로 사용자에게 즉각적인 순위 변동 통지
- 고가용성 : Kafka 3노드 클러스터, Redis Sentinel(마스터-슬레이브), 앱 3대 인스턴스 + Nginx 로드밸런싱
- 하이브리드 승격 : Kafka 소비 시점의 즉시 승격 + 스케줄러 기반 일괄 승격을 결합

---

## 2. 기술 스택 상세

언어 | Kotlin | 1.9.25 
JDK | Java | 17 
프레임워크 | Spring Boot (WebFlux) | 3.5.4 
비동기 처리 | Kotlinx Coroutines + Project Reactor | 1.6.3 
캐시/큐 저장소 | Redis Sentinel | bitnami/redis:latest 
분산 락 | Redisson | 3.18.0 
메시징 | Apache Kafka (KRaft 모드) | confluentinc/cp-kafka:latest
실시간 통신 | SSE (Server-Sent Events)
로드밸런서 | Nginx | latest
보안 | Spring Security WebFlux | Spring Boot 연동
모니터링 | Prometheus + Grafana + Micrometer | Actuator 연동 

---

## 3. 프로젝트 디렉토리 구조

```
src/main/kotlin/com/example/integrated/
├── IntegratedApplication.kt            # 메인 애플리케이션
├── BaseTime.kt                         # 생성/수정 시각 추적 추상 클래스
│
├── queue/                              # ★ 핵심 대기열 시스템
│   ├── queue/                          # 큐 핵심 로직
│   │   ├── QueueController.kt          # REST API 엔드포인트
│   │   ├── QueueService.kt             # 대기열/참가열 비즈니스 로직
│   │   ├── QueueToAllowScheduler.kt    # 만료 정리 + 일괄 승격 스케줄러
│   │   └── dto/
│   │       ├── QueueRequest.kt         # 요청 DTO (queueType, userId)
│   │       └── RegisterResult.kt       # 등록 결과 Enum
│   │
│   ├── kafka/                          # Kafka 메시징 계층
│   │   ├── config/
│   │   │   ├── KafkaProducerConfig.kt  # 프로듀서 설정 (멱등, acks=all)
│   │   │   └── KafkaConsumerConfig.kt  # 컨슈머 설정 (배치, 수동 커밋)
│   │   ├── service/
│   │   │   ├── KafkaProducerService.kt # 메시지 발행
│   │   │   └── KafkaConsumerService.kt # 메시지 소비 (배치 + 코루틴 병렬 + 하이브리드 승격)
│   │   └── dto/
│   │       └── KafkaMessageDto.kt      # Kafka 메시지 DTO
│   │
│   ├── duplication/                    # 멱등성 보장
│   │   └── DuplicationCheckService.kt  # Redis SET NX 기반 중복 요청 감지
│   │
│   └── sse/                            # 실시간 이벤트 스트림
│       ├── SseEventController.kt       # SSE 엔드포인트
│       ├── SseEventService.kt          # SSE 이벤트 생성 로직
│       └── event/
│           ├── SseEvent.kt             # Sealed 클래스 (이벤트 타입 계층)
│           ├── UpdateSseEvent.kt       # 순위 업데이트 이벤트
│           ├── ConfirmSseEvent.kt      # 참가 확정 이벤트
│           └── ErrorSseEvent.kt        # 오류 이벤트
│
├── redis/                              # Redis 인프라 계층
│   ├── RedisLockUtil.kt                # Redisson 분산 락 유틸리티
│   ├── config/
│   │   └── RedisConfig.kt             # Redis Sentinel + Redisson + Pub/Sub 설정
│   └── pubsub/
│       ├── RedisPublisher.kt           # 채널 메시지 발행
│       └── RedisMessageListenerService.kt # 채널 구독 + SSE 연동
│
├── security/
│   └── SecurityConfig.kt              # CORS, 인증 설정
│
├── reserveException/                   # 예외 처리 계층
│   ├── ErrorCode.kt                    # 에러 코드 Enum
│   ├── ReserveException.kt             # 커스텀 예외 클래스
│   ├── ErrorCodeDto.kt                 # 에러 응답 DTO
│   └── CustomExceptionHandler.kt       # 글로벌 예외 핸들러
│
└── util/                               # 공통 유틸리티
    ├── AppUtils.kt                     # 상수 정의 + JSON 확장 함수
    └── Loggable.kt                     # 로깅 인터페이스

src/main/resources/scripts/             # Redis Lua 스크립트
    ├── generate-score.lua              # Score 생성 + 존재 확인 + 등록 플래그 (원자적)
    └── enqueue-or-allow.lua            # 하이브리드 승격 판단 (만료 정리 + 즉시 삽입 판단)
```

---

## 4. 전체 요청 흐름 (End-to-End)

### 4.1. 사용자 등록 흐름

클라이언트 → Nginx → QueueController → QueueService → KafkaProducer → Kafka → KafkaConsumer → 하이브리드 승격 판단 → Redis → Pub/Sub → SSE

#### Step 1: 클라이언트 요청 → Nginx
- 클라이언트가 `POST /queue/register`로 요청 전송 (`request-key` 헤더에 멱등성 키를 포함 )
- Nginx가 `X-Request-Timestamp` 헤더에 밀리초 단위 타임스탬프(`$msec`)를 자동 삽입
- Nginx는 `queueing-cluster` upstream에 라운드로빈 방식으로 분배 ( 3개의 인스턴스 )

#### Step 2: QueueController
- `QueueController.registerUser()`가 요청을 수신
- `request-key` 헤더 존재 여부 검증 → 없으면 `ReserveException` 발생
- `X-Request-Timestamp` 헤더에서 Nginx 도착 시각 추출
- `QueueService.registerUserToWaitQueue()` 호출

#### Step 3: QueueService — 검증 및 Kafka 발행

3-1. 중복 요청 확인 (`DuplicationCheckService`):
- Redis `SET NX` + TTL 5분으로 원자적 중복 감지
- `duplication:{requestKey}` 키가 이미 존재하면 `RegisterResult.DUPLICATE_REQUEST` 반환

3-2. Lua 스크립트로 존재 확인 + Score 생성 (`generate-score.lua`):

단일 Redis 호출로 아래 4가지를 "원자적"으로 수행:
1. `ZSCORE`로 대기열에 이미 존재하는지 확인 → 존재하면 `-1` 반환
2. `ZSCORE`로 참가열에 이미 존재하는지 확인 → 존재하면 `-2` 반환
3. `SET NX EX 10`으로 등록 진행 중 플래그 설정 → 이미 존재하면 `-3` 반환
4. `INCR`로 밀리초별 독립 카운터 증가 + `PEXPIRE 3초` → score 계산 후 반환

Score 공식: `(Nginx 타임스탬프 ms) × 1000 + (밀리초별 독립 카운터)`

3-3. Kafka 이벤트 발행:
- `KafkaMessageDto`를 JSON 직렬화하여 `queueing-system` 토픽에 발행
- `CompletableFuture.await()`로 코루틴 호환 비동기 전송
- 실패 시 `KAFKA_PRODUCE_FAILED` 예외 발생

#### Step 4: KafkaConsumer — 하이브리드 승격 판단

배치 + 코루틴 병렬 처리:
- `isBatchListener = true` 설정으로 한 번에 최대 100개 레코드 수신
- 수신된 배치 내 각 레코드를 `async`로 코루틴 병렬 처리
- 모든 처리 완료 후 `acknowledgment.acknowledge()`로 수동 오프셋 커밋

하이브리드 승격 로직 (`enqueue-or-allow.lua` Lua 스크립트):
- Kafka 소비 시점에 `QueueService.enqueueOrAllow()`를 호출
- Lua 스크립트가 원자적으로 아래를 수행:
  1. `ZREMRANGEBYSCORE`로 참가열에서 만료된 사용자 정리
  2. `ZCARD`로 정리 후 참가열 크기 확인
  3. 참가열에 여유가 있으면(`ZCARD < maxCapacity`) → 참가열에 직접 삽입 (반환: `1`)
  4. 참가열이 가득 찼으면 → 대기열에 삽입 (반환: `0`)
  5. 두 경우 모두 `DEL`로 등록 진행 중 플래그 해제

- 참가열 직접 삽입 시(반환 `1`): SSE로 즉시 입장 알림
- 대기열 삽입 시(반환 `0`): 활성 큐 목록에 등록 후 SSE로 순위 알림 → 스케줄러가 이후에 승격

재시도 전략:
- 최대 5회 재시도, 지수 백오프(1s → 2s → 4s → 8s → 12s)
- 모든 재시도 실패 시 `{topicName}-dlt` ( Dead Letter Topic )로 전송

#### Step 5: Redis Pub/Sub → SSE 전달
- `queueing_system` 채널로 `queueType` 문자열 발행
- 모든 인스턴스의 `RedisMessageListenerService`가 수신
- 수신된 `queueType`을 `MutableSharedFlow`에 emit → SSE 스트림으로 전파
- 연결 끊김 시 지수 백오프로 무한 재연결 (최대 10초 간격)

---

### 4.2. 하이브리드 승격 흐름

대기열 → 참가열 승격은 두 가지 경로로 이루어집니다:

#### 경로 A: 즉시 승격 (Kafka 소비 시점)
```
Kafka 소비 → enqueue-or-allow.lua 실행 → 참가열 여유 확인 → 참가열 직접 삽입 → SSE 즉시 알림
```
- 참가열에 자리가 있을 때 0~3초 대기 없이 즉시 입장
- Lua 스크립트 내에서 만료 사용자 정리 후 판단하므로 정확한 여유 계산

#### 경로 B: 스케줄러 일괄 승격 (3초 주기)
```
스케줄러 → 분산 락 획득 → 만료 사용자 정리 → 여유 공간 계산 → 대기열에서 일괄 승격 → SSE 알림
```
- 참가열이 가득 찬 상태에서 대기열에 들어간 사용자를 위한 백업 경로
- 만료된 사용자가 빠져나가면서 여유가 생길 때 일괄로 승격

#### 스케줄러 동작 상세

3초 주기로 아래 순서를 수행 (Redisson 분산 락으로 3개 인스턴스 중 1대만 실행):

1. 만료 사용자 정리: `ZREMRANGEBYSCORE`로 참가열에서 score(=만료시각) < 현재시각인 멤버 삭제
2. 여유 공간 계산: `ZCARD`로 정리 후 참가열 크기 확인, `maxOf(0, maxCapacity - allowQueueSize)`로 음수 방지
3. 일괄 승격: `ZPOPMIN`으로 대기열에서 score가 가장 낮은(가장 먼저 등록된) 사용자부터 여유분만큼 추출 → 참가열에 `ZADD`로 일괄 삽입 (score = 현재시각 + 10분)
4. 비활성화 판단: 대기열이 비어있고 승격 대상이 없으면 활성 큐 목록에서 제거

#### 만료 정리가 두 곳에서 발생하는 이유

| 시점 | 위치 | 목적 |
|------|------|------|
| Kafka 소비 시 | `enqueue-or-allow.lua` | 삽입 직전에 정리하여 정확한 여유 공간 계산 (Lua 스크립트 원자성 보장) |
| 스케줄러 3초 주기 | `cleanExpiredAllowQueueMembers()` | 트래픽이 없을 때도 주기적으로 만료 사용자 정리 (메모리 절약 백업) |

#### 분산 락 상세

| 설정 | 값 | 설명 |
|------|-----|------|
| 키 | `scheduling-key` | Redisson 기반 |
| 대기 시간 | 0초 | 즉시 시도, 실패 시 건너뜀 |
| 유지 시간 | 4초 | 스케줄링 주기(3초)보다 약간 길게 |
| 장애 안전 | 자동 해제 | leaseTime 만료로 데드락 방지 |

---

### 4.3. SSE 실시간 스트림 흐름

```
클라이언트 GET /queue/stream → SseEventController → SseEventService → Redis 조회 → SSE 이벤트 전송
```

- `ConcurrentHashMap<queueType, MutableSharedFlow>`로 큐 타입별 독립적인 이벤트 버스
- `extraBufferCapacity = 64`: 소비자가 느릴 때 최대 64개 이벤트 버퍼링
- Redis Pub/Sub에서 수신된 이벤트가 `tryEmit`으로 Flow에 전달

#### SSE 이벤트 빌드 로직
1. 참가열 존재 확인 (`isAllowTokenExpired` = false): → `confirmed` 이벤트 전송
2. 대기열 순위 조회 (`getWaitQueueRank`): 양수이면 → `update` 이벤트 (현재 순위 포함)
3. 승격 중간 상태 처리: 순위 0 이하 + 참가열 미존재 시 한 번 더 참가열 확인 (race condition 방어)
4. 최종 실패: → `error` 이벤트

#### SSE 이벤트 타입 (Sealed Class 계층)
| 이벤트 | 클래스 | 설명 |
|--------|--------|------|
| `update` | `UpdateSseEvent` | 대기열 순위 갱신 (rank 포함) |
| `confirmed` | `ConfirmSseEvent` | 참가열 입장 확정 (userId 포함) |
| `error` | `ErrorSseEvent` | 오류 발생 (message 포함) |

---

### 4.4. 토큰 인증 및 쿠키 흐름

#### 토큰 생성
- HmacSHA256으로 `queueType:userId` 서명
- `validationKey`(설정 파일에서 주입)을 비밀키로 사용
- URL-safe Base64 인코딩 (패딩 없음)

#### 쿠키 발급 (`GET /queue/create/cookie`)
- 참가열에 유효하게 존재하는지 먼저 확인
- 쿠키명: `reserve-user-access-cookie-{userId}` (URL 인코딩)
- 토큰을 쿠키에 저장, `maxAge = 600초 (10분)`

#### 토큰 검증 (`POST /queue/isValidateToken/{token}`)
- 이중 검증: ① 참가열 만료 여부 (Redis score < 현재시각) + ② 토큰 불일치 여부
- 참가열의 score가 만료 타임스탬프이므로, score < 현재시각이면 만료

---

### 4.5. 대기열 취소 흐름

- `cancelUser()` 호출 시 대기열과 참가열에서 코루틴 병렬 삭제 시도
- 둘 중 하나라도 삭제 성공하면 `true` 반환
- Redis `ZREM` 명령으로 Sorted Set에서 멤버 제거

---

## 5. Redis 데이터 구조 상세

### 5.1. 대기열 (Wait Queue)
- 키 형식: `{queueType}:user-queue:wait`
- 타입: Sorted Set (ZSET)
- member: `userId`
- score: `(Nginx 도착시각 ms) × 1000 + (밀리초별 독립 카운터)`
- 의미: score가 작을수록 먼저 등록 → 먼저 승격

### 5.2. 참가열 (Allow Queue)
- 키 형식: `{queueType}:user-queue:allow`
- 타입: Sorted Set (ZSET)
- member: `userId`
- score: `System.currentTimeMillis() + 600,000` (현재시각 + 10분)
- 의미: score = 만료 시각, score < 현재시각이면 만료로 간주
- 만료 정리: `ZREMRANGEBYSCORE`로 score ≤ 현재시각인 멤버를 주기적으로 삭제

### 5.3. 활성 큐 플래그
- 키: `active-allow-queue`
- 타입: Set
- 멤버: 현재 스케줄링 대상인 `queueType` 문자열들
- 역할: 스케줄러가 빈 큐를 불필요하게 순회하지 않도록 최적화

### 5.4. 밀리초별 독립 카운터
- 키 형식: `queue:seq:{timestampMs}`
- 타입: String (Integer)
- TTL: 3초 (PEXPIRE, 첫 요청 시 자동 설정)
- 역할: Lua 스크립트 내에서 `INCR`로 원자적 증가, 동일 밀리초 내 순서 구분용
- 이전 방식과의 차이: 전역 카운터의 `seq % 1000` 모듈로 연산에서 발생하던 순서 역전 버그를 제거

### 5.5. 등록 진행 중 플래그
- 키 형식: `registering:{queueType}:{userId}`
- 타입: String, 값 `"1"`
- TTL: 10초 (SET NX EX)
- 역할: Kafka 소비 전 동일 사용자의 동시 등록을 원자적으로 차단
- 해제 시점: `enqueue-or-allow.lua`에서 대기열/참가열 삽입 완료 후 `DEL`로 명시적 해제

### 5.6. 중복 요청 방지
- 키 형식: `duplication:{requestKey}`
- 타입: String, 값 `"1"`
- TTL: 5분
- 역할: `SET NX`로 멱등성 보장

### 5.7. Pub/Sub 채널
- 채널명: `queueing_system`
- 메시지: `queueType` 문자열
- 발행 시점: Kafka 소비 후(즉시 승격/대기열 삽입 모두), 스케줄러 일괄 승격 시

### 5.8. 분산 락
- 키: `scheduling-key`
- TTL: 4초 (leaseTime)
- 역할: 스케줄러 다중 실행 방지

---

## 6. Lua 스크립트 상세

시스템은 2개의 Redis Lua 스크립트를 사용하여 원자적 연산을 수행합니다.

### 6.1. `generate-score.lua` — Score 생성 + 존재 확인

호출 시점: 사용자 등록 요청 시 (`QueueService.executeScoreGeneration()`)

수행 내용 (원자적):
1. 대기열에 이미 존재하는지 확인 (`ZSCORE`) → 존재하면 `-1`
2. 참가열에 이미 존재하는지 확인 (`ZSCORE`) → 존재하면 `-2`
3. 등록 진행 중 플래그 설정 (`SET NX EX 10`) → 이미 존재하면 `-3`
4. 밀리초별 독립 카운터 증가 (`INCR`) + TTL 설정 (`PEXPIRE 3초`)
5. score 계산 후 반환: `timestampMs × 1000 + seq`

설계 이유:
- 존재 확인과 Score 생성을 분리하면 TOCTOU 레이스 컨디션 발생 가능
- 등록 진행 중 플래그로 Kafka 소비 전 동일 사용자의 동시 등록 차단
- 밀리초별 독립 카운터로 전역 `seq % 1000`의 순서 역전 버그 제거

### 6.2. `enqueue-or-allow.lua` — 하이브리드 승격 판단

호출 시점: Kafka 소비 시 (`QueueService.enqueueOrAllow()`)

수행 내용 (원자적):
1. 참가열에서 만료된 사용자 정리 (`ZREMRANGEBYSCORE -inf nowMs`)
2. 정리 후 참가열 크기 확인 (`ZCARD`)
3. 참가열에 여유가 있으면 → 참가열에 직접 삽입 (`ZADD`, score = expireAt) → 반환 `1`
4. 참가열이 가득 찼으면 → 대기열에 삽입 (`ZADD`, score = waitScore) → 반환 `0`
5. 두 경우 모두 등록 진행 중 플래그 해제 (`DEL`)

설계 이유:
- 만료 정리 + 크기 확인 + 삽입이 원자적이므로 TOCTOU 문제 없음
- 여러 Kafka 소비자 인스턴스가 동시에 실행해도 Redis 단일 스레드 특성상 안전
- 참가열에 여유가 있을 때 0~3초 대기 없이 즉시 입장 가능

---

## 7. Kafka 구성 상세

### 7.1. 토픽 구성
| 토픽명 | 파티션 | 복제 팩터 | 용도 |
|--------|--------|-----------|------|
| `queueing-system` | 3 | 3 | 대기열 등록 이벤트 |
| `queueing-system-dlt` | 자동 | 자동 | 처리 실패 메시지 (Dead Letter) |

### 7.2. 프로듀서 설정
| 설정 | 값 | 설명 |
|------|-----|------|
| `acks` | `all` | 모든 ISR 브로커 확인 |
| `enable.idempotence` | `true` | 멱등 프로듀서 (중복 발행 방지) |
| `max.in.flight.requests` | `5` | 멱등 모드에서 5까지 순서 보장 안전 |
| `linger.ms` | `5` | 5ms 배치 대기 |
| `batch.size` | `32768` | 32KB 배치 |
| `retries` | `MAX_VALUE` | 무제한 재시도 |
| `delivery.timeout.ms` | `120000` | 전송 타임아웃 2분 |
| `retry.backoff.ms` | `100` | 재시도 간격 100ms |

### 7.3. 컨슈머 설정
| 설정 | 값 | 설명 |
|------|-----|------|
| `enable.auto.commit` | `false` | 수동 오프셋 커밋 |
| `max.poll.records` | `100` | 한 번에 100개 레코드 |
| `max.poll.interval.ms` | `300000` | 5분 폴링 타임아웃 |
| `session.timeout.ms` | `30000` | 30초 세션 타임아웃 |
| `isBatchListener` | `true` | 배치 리스너 모드 |
| `ackMode` | `MANUAL_IMMEDIATE` | 수동 즉시 커밋 |
| `concurrency` | `1` | 파티션 순서 보장 |

### 7.4. 컨슈머 재시도 전략
```
시도 1 → 실패 → 1초 대기
시도 2 → 실패 → 2초 대기
시도 3 → 실패 → 4초 대기
시도 4 → 실패 → 8초 대기
시도 5 → 실패 → 12초 대기
시도 6 → 실패 → DLT 전송 (queueing-system-dlt)
```

---

## 8. 인프라 아키텍처 (Docker Compose)

### 8.1. 전체 토폴로지
```
                           ┌─────────────────┐
                           │   클라이언트      │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │  Nginx (:8079)   │
                           │  (라운드로빈)      │
                           └──┬─────┬─────┬──┘
                              │     │     │
                    ┌─────────▼─┐ ┌─▼───┐ ┌▼─────────┐
                    │ App-1     │ │App-2│ │ App-3     │
                    │ (:8081)   │ │(:8082)│ │ (:8083)  │
                    └─────┬─────┘ └──┬──┘ └─────┬─────┘
                          │          │          │
           ┌──────────────┼──────────┼──────────┼──────────────┐
           │              │          │          │              │
    ┌──────▼──────┐ ┌─────▼─────┐ ┌─▼──────┐ ┌─▼──────────┐  │
    │ Kafka-1     │ │ Kafka-2   │ │Kafka-3 │ │Redis Master│  │
    │ (:9092)     │ │ (:9093)   │ │(:9094) │ │ (:6379)    │  │
    └─────────────┘ └───────────┘ └────────┘ └──┬────┬────┘  │
                                                 │    │       │
                                          ┌──────▼┐ ┌▼──────┐│
                                          │Slave01│ │Slave02││
                                          │(:6479)│ │(:6579)││
                                          └───────┘ └───────┘│
                                                              │
                                    ┌─────────┬───────┬───────┘
                                    │         │       │
                              ┌─────▼───┐┌────▼──┐┌──▼──────┐
                              │Sentinel1││Sent. 2││Sentinel3│
                              │(:26379) ││(:26380)│(:26381) │
                              └─────────┘└───────┘└─────────┘
```

### 8.2. 모니터링 스택
```
App-1,2,3 (:9292/actuator/prometheus) ──┐
                                         ├──→ Prometheus (:9090) ──→ Grafana (:3001)
Kafka-Exporter (:9308) ────────────────┘
```

- Actuator 포트: 9292 (메인 앱과 분리)
- Kafka Exporter: Consumer Lag 등 Kafka 메트릭 수집

### 8.3. Nginx 설정 핵심
- `X-Request-Timestamp: $msec` — 순서 보장의 핵심, 모든 인스턴스에서 동일 기준
- `keepalive 32` — upstream 커넥션 풀링
- `proxy_buffering off` — SSE 스트리밍에 필수 (버퍼링하면 이벤트 지연)
- `proxy_read_timeout 3600s` — SSE 연결 유지 (1시간)

### 8.4. Kafka 클러스터 (KRaft 모드)
- ZooKeeper 없음: KRaft 합의 프로토콜 사용
- 각 노드가 `broker` + `controller` 역할 겸임
- 동일 `CLUSTER_ID`로 클러스터 구성
- 컨트롤러 쿼럼: 3노드 투표
- 기본 파티션 3개, 복제 팩터 3

### 8.5. Redis Sentinel 구성
| 설정 | 값 | 설명 |
|------|-----|------|
| `DOWN_AFTER_MILLISECONDS` | 5000 | 5초 무응답 시 장애 판단 |
| `FAILOVER_TIMEOUT` | 10000 | Failover 10초 타임아웃 |
| `PARALLEL_SYNCS` | 1 | 동시 sync 슬레이브 1개 |
| `QUORUM` | 2 | 장애 확정에 2개 Sentinel 동의 필요 |
| `MASTER_SET` | `toMaster` | Sentinel 마스터 이름 |

---

## 9. REST API 엔드포인트

모든 엔드포인트는 `/queue` 경로 하위에 위치합니다.

| 메서드 | 경로 | 설명 | 파라미터 | 응답 |
|--------|------|------|----------|------|
| `POST` | `/register` | 대기열 등록 | Body: `{queueType, userId}`, Header: `request-key`, `X-Request-Timestamp` | `RegisterResult` |
| `GET` | `/get/rank` | 순위 조회 | Query: `queueType`, `userId`, `queueCategory`(wait/allow) | `Long` (1-based, 없으면 -1) |
| `GET` | `/create/cookie` | 접근 토큰 쿠키 발급 | Query: `queueType`, `userId` | 200 + Set-Cookie |
| `POST` | `/isValidateToken/{token}` | 토큰 유효성 검증 | Body: `{queueType, userId}`, Path: `token` | `Boolean` |
| `POST` | `/cancel` | 대기열/참가열 취소 | Body: `{queueType, userId}` | `Boolean` |
| `GET` | `/stream` | SSE 실시간 스트림 | Query: `queueType`, `userId` | `Flow<ServerSentEvent>` |

---

## 10. 보안 설정

- CSRF 비활성화 (WebFlux API 서버)
- `/queue/`, `/actuator/` 공개 접근 허용, 그 외 인증 필요
- CORS: `http://localhost:3000` (프론트엔드), GET/POST/PUT/DELETE/OPTIONS 허용, `allowCredentials = true`

---

## 11. 예외 처리 체계

### ErrorCode Enum
| 코드 | 메시지 | 사용 위치 |
|------|--------|-----------|
| `UNKNOWN` | 알 수 없는 에러 발생 | 기본 |
| `NOT_EXIST_IN_HEADER_REQUEST_KEY` | REQUEST_KEY가 Header에 존재하지 않습니다 | QueueController |
| `INVALID_QUEUE_CATEGORY` | 유효하지 않은 QUEUE CATEGORY 입니다 | - |
| `ALREADY_REGISTERED_USER_IN_QUEUE` | 이미 등록된 사용자입니다 | - |
| `FAIL_TO_REGISTER` | 대기열 등록 실패 | - |
| `REDIS_OPERATION_FAILED` | Redis 작업 중 실패 | - |
| `FAIL_TO_GENERATE_TOKEN` | 토큰 생성 중 에러 발생 | QueueService |
| `KAFKA_PRODUCE_FAILED` | Kafka Produce 실패 | QueueService |
| `FAILED_TO_STORE_TOKEN_IN_COOKIE` | 쿠키에 토큰 저장 실패 | QueueService |

### 예외 응답 형식
```json
{
    "code": "KAFKA_PRODUCE_FAILED",
    "message": "Kafka Produce 실패",
    "detail": null
}
```
- `@ControllerAdvice`로 글로벌 처리
- `ReserveException` → `ErrorCodeDto` 변환 → `ResponseEntity` 반환

---

## 12. Score 생성 알고리즘

### 문제
- 3대의 앱 인스턴스가 동시에 요청을 받으므로, 각 인스턴스의 시스템 시계로는 정확한 순서 보장 불가
- 동일 밀리초에 다수 요청이 도달할 수 있음
- Score 생성과 실제 대기열 삽입 사이에 Kafka를 거치므로, 체크-삽입 간 시간 간극이 존재

### 해결: Redis Lua 스크립트 기반 원자적 Score 생성

```
score = (Nginx 도착 밀리초) × 1000 + (밀리초별 독립 카운터)
```

| 구성 요소 | 역할 | 범위 |
|-----------|------|------|
| Nginx `$msec` | 단일 진입점의 통일된 시각 | 밀리초 정밀도 |
| 밀리초별 카운터 `queue:seq:{ms}` | 동일 밀리초 내 순서 구분 | 1부터 시작, 역전 없음 |
| 등록 진행 중 플래그 | Kafka 소비 전 동일 사용자 중복 차단 | TTL 10초 |
| 최종 score | 전역 고유 순서값 | Long → Double 변환 (2^53 범위 내 안전) |

### 이전 방식 대비 개선점

| 항목 | 이전 (전역 seq % 1000) | 현재 (Lua + 밀리초별 카운터) |
|------|----------------------|---------------------------|
| 순서 역전 | seq가 1000 경계를 넘을 때 역전 발생 가능 | 밀리초별 독립 카운터로 역전 불가 |
| 원자성 | 체크와 생성이 분리되어 TOCTOU 레이스 컨디션 존재 | Lua 스크립트로 전체 원자적 실행 |
| 동시 등록 방지 | 체크-삽입 간 Kafka 간극에서 중복 가능 | 등록 진행 중 플래그로 원자적 차단 |
| Redis 왕복 | 3회 (INCR + ZRANK × 2) | 1회 (Lua 스크립트) |
| 카운터 메모리 | 영구 키 1개 (queue:seq) | 밀리초별 키, 3초 TTL로 자동 정리 |

### 예시
```
같은 밀리초(ts=1740000000000)에 3개 요청 도착:
사용자 A: seq=1 → score = 1740000000000 × 1000 + 1 = 1740000000000001
사용자 B: seq=2 → score = 1740000000000 × 1000 + 2 = 1740000000000002
사용자 C: seq=3 → score = 1740000000000 × 1000 + 3 = 1740000000000003
→ A < B < C 순서 보장, 역전 불가
```

### Double 정밀도 안전성
- 현재 시점 score 최대값: ~1.74 × 10^15
- Double 정확한 정수 표현 한계: 2^53 ≈ 9.0 × 10^15
- 현재 시점에서 약 19% 사용, 2255년까지 안전

### Nginx `$msec`를 유지하는 이유
순수 Redis INCR만 사용하면 인스턴스 간 처리 속도 차이로 순서가 왜곡될 수 있습니다:
```
T=100.0ms: 사용자 A → Nginx → App-1 (GC 일시 정지)
T=100.5ms: 사용자 B → Nginx → App-2 (즉시 처리)
→ Redis INCR만 사용 시: B가 먼저 도달하여 낮은 score 획득 (불공정)
→ Nginx $msec 사용 시: A의 타임스탬프가 더 작으므로 정확한 순서 보장
```

---

## 13. 분산 환경에서의 일관성 보장 전략

### 13.1. 중복 요청 방지 (멱등성)
| 계층 | 메커니즘 | 보장 수준 |
|------|----------|-----------|
| API 게이트웨이 | `request-key` 헤더 + Redis `SET NX` (5분 TTL) | 클라이언트 레벨 멱등성 |
| Kafka 프로듀서 | `ENABLE_IDEMPOTENCE = true` | 브로커 레벨 중복 발행 방지 |
| Lua 스크립트 | `ZSCORE` 대기열/참가열 존재 확인 (원자적) | 비즈니스 레벨 중복 등록 방지 |
| 등록 진행 중 플래그 | `SET NX registering:{queueType}:{userId}` (TTL 10초) | Kafka 소비 전 동시 등록 방지 |

### 13.2. 순서 보장
| 계층 | 메커니즘 |
|------|----------|
| 요청 수신 | Nginx `$msec` 타임스탬프 통일 |
| Score 생성 | Lua 스크립트: Nginx 시각 × 1000 + 밀리초별 독립 카운터 (원자적) |
| 대기열 저장 | Redis Sorted Set (score 기반 자동 정렬) |
| 승격 순서 | `ZPOPMIN`으로 최소 score부터 원자적 추출 |

### 13.3. 장애 복구
| 장애 시나리오 | 복구 메커니즘 |
|--------------|-------------|
| Kafka 프로듀서 실패 | 무제한 재시도(2분 타임아웃) → 예외 반환 |
| Kafka 컨슈머 실패 | 5회 지수 백오프 재시도 → DLT 전송 |
| Redis Pub/Sub 연결 끊김 | 지수 백오프 무한 재연결 (최대 10초) |
| 스케줄러 인스턴스 장애 | 다음 주기에 다른 인스턴스가 락 획득 |
| Redis 마스터 장애 | Sentinel Failover (5초 감지, 10초 전환) |
| Kafka 브로커 장애 | 복제 팩터 3으로 데이터 보존, 리밸런싱 |

---

## 14. 설정 값 요약

| 설정 | 값 | 설명 |
|------|-----|------|
| `server.port` | 8081 | 앱 내부 포트 |
| `queue.allow.max-capacity` | 10,000 | 참가열 최대 용량 |
| `queue.allow.interval-ms` | 3,000 | 승격 스케줄링 주기 (3초) |
| `queue.validation.key` | `park` | HmacSHA256 서명 키 |
| `queue.event.topic.name` | `queueing-system` | Kafka 토픽명 |
| `management.server.port` | 9292 | Actuator 전용 포트 |
| Nginx 포트 | 8079 (외부) → 90 (내부) | 로드밸런서 진입점 |
| 중복 방지 TTL | 5분 | request-key 유효기간 |
| 참가열 TTL | 10분 | 입장 토큰 유효기간 |
| 쿠키 TTL | 600초 (10분) | 접근 토큰 쿠키 유효기간 |
| 분산 락 leaseTime | 4초 | 스케줄러 락 자동 해제 |
| Kafka 소비자 배치 | 100개 | 한 번에 가져오는 레코드 수 |
| Kafka 소비자 재시도 | 5회 | 지수 백오프 (1s~12s) |

---

## 15. 의존성 그래프

```
QueueController
    └── QueueService
            ├── KafkaProducerService
            │       └── KafkaTemplate (KafkaProducerConfig)
            ├── DuplicationCheckService
            │       └── ReactiveRedisTemplate
            ├── ReactiveRedisTemplate
            │       ├── generate-score.lua (Score 생성)
            │       └── enqueue-or-allow.lua (하이브리드 승격)
            └── RedisPublisher
                    └── ReactiveRedisTemplate

KafkaConsumerService (비동기 소비)
    ├── QueueService (enqueueOrAllow → 하이브리드 승격)
    ├── QueueToAllowScheduler (addActiveQueue)
    ├── RedisPublisher
    └── KafkaTemplate (DLT 전송용)

QueueToAllowScheduler (3초 주기)
    ├── QueueService (allowUser → 일괄 승격)
    ├── RedisLockUtil
    │       └── RedissonClient (RedisConfig)
    └── ReactiveRedisTemplate
            └── cleanExpiredAllowQueueMembers (만료 정리)

SseEventController
    └── SseEventService
            ├── QueueService
            └── MutableSharedFlow (static ConcurrentHashMap)

RedisMessageListenerService (@PostConstruct)
    ├── ReactiveRedisMessageListenerContainer (RedisConfig)
    └── SseEventService.getSink() (static)
```

---

## 16. 데이터 흐름 시퀀스 다이어그램

### 등록 → 하이브리드 승격 → 입장 전체 시퀀스

```
Client          Nginx           App             Kafka           Redis           SSE
  │               │               │               │               │              │
  │──POST /register──▶│            │               │               │              │
  │               │──X-Request-Timestamp──▶│       │               │              │
  │               │               │               │               │              │
  │               │               │──SET NX duplication:key──────▶│              │
  │               │               │◀─────────── OK ──────────────│              │
  │               │               │                               │              │
  │               │               │──generate-score.lua ────────▶│              │
  │               │               │  (존재 확인 + 플래그 + Score)   │              │
  │               │               │◀─────────── score ───────────│              │
  │               │               │                               │              │
  │               │               │──produce(queueing-system)──▶│              │
  │               │               │◀────────── ack ─────────────│              │
  │◀──────── SUCCESS ────────────│               │               │              │
  │               │               │               │               │              │
  │               │               │      consume(batch)          │              │
  │               │               │◀──────────────│               │              │
  │               │               │                               │              │
  │               │               │──enqueue-or-allow.lua ──────▶│              │
  │               │               │  (만료정리 + 여유확인 + 삽입)   │              │
  │               │               │◀─────────── result ──────────│              │
  │               │               │                               │              │
  │               │               │  [result=1: 참가열 직접 삽입]   │              │
  │               │               │──PUBLISH queueing_system────▶│              │
  │               │               │               │               │──▶ Listener  │
  │               │               │               │               │    tryEmit──▶│
  │               │               │               │               │              │──SSE confirmed──▶Client
  │               │               │                               │              │
  │               │               │  [result=0: 대기열 삽입]       │              │
  │               │               │──SADD active-allow-queue────▶│              │
  │               │               │──PUBLISH queueing_system────▶│              │
  │               │               │               │               │──▶ Listener  │
  │               │               │               │               │    tryEmit──▶│
  │               │               │               │               │              │──SSE update(rank)──▶Client
  │               │               │               │               │              │
  │               │         [3초 후 스케줄러]        │               │              │
  │               │               │──tryLock scheduling-key───▶│              │
  │               │               │◀───────── locked ──────────│              │
  │               │               │──ZREMRANGEBYSCORE (만료정리)─▶│              │
  │               │               │──ZCARD allow queue ─────────▶│              │
  │               │               │──ZPOPMIN wait queue ────────▶│              │
  │               │               │──ZADD allow queue ──────────▶│              │
  │               │               │──PUBLISH queueing_system────▶│              │
  │               │               │               │               │──▶ Listener  │
  │               │               │               │               │    tryEmit──▶│
  │               │               │               │               │              │──SSE confirmed──▶Client
```
