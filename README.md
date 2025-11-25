### 프로젝트 설명

---

기존의 예약 시스템과 대기열 시스템을 개선하여 하나로 통합한 프로젝트입니다.

이 리포지토리에서는 개선된 대기열 시스템에 대한 내용을 담고 있습니다.<br><br>

**구현 과정**<br>

https://velog.io/@ayeah77/series/%EB%8C%80%EA%B8%B0%EC%97%B4-%EC%8B%9C%EC%8A%A4%ED%85%9C-%EA%B0%9C%EC%84%A0<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : Redis

Infra : Kafka, Docker

Etc : SSE<br><br>

### 기존 대기열 시스템에서의 개선점

---

기존 대기열 시스템에서는 MySQL과 Kafka Debezium Connector를 활용하여 CDC 방식으로 이벤트를 전달하였으나, 이는 데이터베이스에 지나치게 의존적이었고 특정 지점에서 단일 장애점( SPOF )이 발생할 가능성이 있었습니다. 

그러나 이 프로젝트의 특성상 이벤트 전달을 위해 굳이 데이터베이스를 거칠 필요가 없었기 때문에, WebFlux 기반의 비동기 구조에 더 적합한 Kafka 직접 발행 방식으로 전환하였습니다.

시스템을 단일 서버에서 분산 서버 구조로 확장함에 따라, 서버 간 SSE Sink 데이터의 일관성을 유지하기 위해 처음에는 Redis Pub/Sub을 사용하였습니다.
Redis Pub/Sub으로도 메시지 전달은 가능하지만, 메시지 손실에 대한 보장이나 내결함성 측면에서는 한계가 있습니다. 이에 따라 영속성과 신뢰성이 보장되는 Kafka로 전환하여 안정적인 데이터 일관성을 확보하였습니다.

결과적으로 개선된 구조는 데이터베이스 의존성을 제거하여 단일 장애점을 제거하였으며, 비동기/분산 환경에 적합한 이벤트 전송 및 서버 간 동기화를 가능하게 하였습니다.<br><br>

### 아키텍처 및 대기열 등록 과정

---

[ 단일 서버 ]

<img width="622" height="232" alt="Image" src="https://github.com/user-attachments/assets/400df547-e600-4093-aa9c-19c2bd2bbccf" />

**단일 서버에서의 대기열 등록 과정**
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이 과정에서 SSE Sink 구독이 이루어집니다.
2. Kafka의 대기열 토픽에 메시지를 발행( Produce )합니다.
3. Kafka Consume을 통해 SSE sink로 전송되며, 사용자의 실시간 순위 및 예약 페이지 접근 권한을 전달받게 됩니다.<br><br>

[ 분산 서버 ]

<img width="890" height="337" alt="Image" src="https://github.com/user-attachments/assets/ae1f1f2a-eead-4f7b-9452-07978f767875" />
<br><br>

**분산 서버에서의 대기열 등록 과정**<br><br>

**기본 설정**

대기열 프로젝트는 Docker를 사용한 분산 환경으로 이루어져 있습니다.

애플리케이션이 시작될 때 각 서버는 특정 redis 채널을 구독합니다.<br><br>

1. 대기열 등록 요청이 들어오면, 클라이언트는 어떤 대기열인지를 나타내는 queueType, 사용자 ID인 userId, 멱등키인idempotencyKey를 서버로 전달합니다.
    
    이때 요청이 도착한 시각을 서버에서 timestamp로 기록하며, 클라이언트는 서버와 SSE 연결을 맺습니다.
    
2. 서버는 먼저 DB에서 idempotencyKey를 조회하여 존재하지 않는 경우에만 queueType, userId, timestamp 정보를 Kafka 로 publish 합니다.
3. 여러 서버는 동일한 consumer group으로 구성되어 있기 때문에, Kafka 메시지는 한 서버에서만 consume 하며, 이때  Redis Sorted Set 자료구조로 이루어진 대기열에 userId를 key로, timestamp를 score로 저장하여, 자동으로 대기열 순서가 정렬되도록 합니다.
4. 이후 메시지를 consume 한 서버는, 모든 서버가 구독하고 있던 Redis Pub/Sub 채널에 해당 queueType 값을 publish하여, 다른 모든 서버도 해당 대기열에서 변화가 발생했음을 실시간으로 감지하도록 합니다.
5. 클라이언트는 SSE 연결을 통해 대기열의 상태가 갱신될 때마다 자신의 대기열 상태를 지속적으로 전달받습니다.
6. Redis는 대기열과 참가열 두 영역으로 구성되며, 스케줄러가 주기적으로 대기열에서 일정 인원을 참가열로 이동시킵니다. 참가열로 이동된 사용자는 타깃 페이지에 접근할 수 있는 권한을 획득하여 해당 페이지로 이동하게 됩니다.<br><br>

**예약 동작 과정**

- Redis Redisson Lock을 사용하여 동일 좌석 예약 시 단일 요청만 처리되도록 하여 동시성 문제를 방지했습니다.
- 멱등성을 적용해 짧은 시간에 반복된 동일 요청( 따닥 이슈 )을 방지하고, 이미 처리된 요청은 거절하며 기존 결과를 반환하도록 했습니다.

- 예약 프로젝트 리포지토리 : https://github.com/TaeHyun77/integrated-reserve-back.git<br><br>
