### 프로젝트 설명

---

기존의 단일 대기열 시스템을 분산 환경으로 확장하여 개선한 프로젝트입니다.<br><br>

**구현 과정**<br>

https://velog.io/@ayeah77/series/%EB%8C%80%EA%B8%B0%EC%97%B4-%EC%8B%9C%EC%8A%A4%ED%85%9C-%EA%B0%9C%EC%84%A0<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : Redis

Infra : Kafka, Docker

Etc : SSE, Redis pub/sub<br><br>

### 기존 대기열 시스템에서의 개선점

---

기존 대기열 시스템에서는 특정 대기열에서 변동 사항이 발생할 때, DB 트랜잭션과 이벤트 전송의 정합성을 보장하기 위해 MySQL과 Kafka Debezium Connector를 활용한 CDC 방식으로 이벤트를 전달했습니다.

하지만, 이 방식은 데이터베이스에 과도하게 의존하여 병목이 발생할 수 있었고, 특정 지점에서 단일 장애점(SPOF)이 될 가능성이 존재했습니다.

대기열 프로젝트의 특성상 이벤트 전달을 위해 굳이 데이터베이스를 사용할 필요가 없으며, WebFlux 기반의 비동기 구조에서 동기적인 DB 접근은 적합하지 않기 때문에 해당 요소를 제거하였습니다.

또한, 시스템을 단일 서버에서 분산 서버 구조로 확장하면서 가용성을 높였고, 서버 간 SSE 전달의 일관성을 유지하기 위해 Redis Pub/Sub을 도입했습니다. 이를 통해 각 서버가 동일한 이벤트를 실시간으로 전달하는 것이 가능해졌습니다.<br><br>

### 아키텍처 및 대기열 등록 과정

---

[ 기존의 단일 서버 ]

<p align="center"><img width="625" height="188" alt="Image" src="https://github.com/user-attachments/assets/0205be3b-61b1-4284-a1da-17e9aecd89a4" /><br><br>

**단일 서버에서의 대기열 등록 과정**

단일 서버 리포지토리를 확인해주세요

<br><br>

[ 분산 서버 ]

<img width="890" height="337" alt="Image" src="https://github.com/user-attachments/assets/ae1f1f2a-eead-4f7b-9452-07978f767875" /><br><br>

**기본 설정**

대기열 프로젝트는 Docker를 사용한 분산 환경으로 이루어져 있습니다.

애플리케이션이 시작될 때 각 서버는 특정 redis 채널을 구독합니다.<br><br>

**분산 서버에서의 대기열 등록 과정**<br>

1. 대기열 등록 요청이 들어오면, 클라이언트는 어떤 대기열인지를 나타내는 queueType, 사용자 ID인 userId, 멱등키인idempotencyKey를 서버로 전달합니다.
    
    이때 요청이 도착한 시각을 서버에서 timestamp로 기록하며, 클라이언트는 서버와 SSE 연결을 맺습니다.
    
2. 서버는 먼저 DB에서 idempotencyKey를 조회하여 존재하지 않는 경우에만 queueType, userId, timestamp 정보를 Kafka 로 publish 합니다.
3. 여러 서버는 동일한 consumer group으로 구성되어 있기 때문에, Kafka 메시지는 한 서버에서만 consume 하며, 이때  Redis Sorted Set 자료구조로 이루어진 대기열에 userId를 key로, timestamp를 score로 저장하여, 자동으로 대기열 순서가 정렬되도록 합니다.
4. 이후 메시지를 consume 한 서버는, 모든 서버가 구독하고 있던 Redis Pub/Sub 채널에 해당 queueType 값을 publish하여, 다른 모든 서버도 해당 대기열에서 변화가 발생했음을 실시간으로 감지하도록 합니다.
5. 클라이언트는 SSE 연결을 통해 대기열의 상태가 갱신될 때마다 자신의 대기열 상태를 지속적으로 전달받습니다.
6. Redis는 대기열과 참가열 두 영역으로 구성되며, 스케줄러가 주기적으로 대기열에서 일정 인원을 참가열로 이동시킵니다. 참가열로 이동된 사용자는 타깃 페이지에 접근할 수 있는 권한을 획득하여 해당 페이지로 이동하게 됩니다.<br><br>
