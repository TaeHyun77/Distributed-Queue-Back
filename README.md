### 프로젝트 설명

---

기존의 단일 대기열 시스템을 분산 환경으로 확장하여 개선한 프로젝트입니다.<br><br>

**구현 과정 블로그**<br>

https://velog.io/@ayeah77/series/%EB%8C%80%EA%B8%B0%EC%97%B4-%EC%8B%9C%EC%8A%A4%ED%85%9C-%EA%B0%9C%EC%84%A0<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : Redis

Infra : Kafka, Docker

Etc : SSE, Redis pub/sub<br><br>

### 기존 단일 대기열 시스템에서의 변경점

---

기존 대기열 시스템에서는 특정 대기열에서 변동 사항이 발생할 때, DB 트랜잭션과 이벤트 전송의 정합성을 보장하기 위해 MySQL과 Kafka Debezium Connector를 활용한 CDC 방식으로 이벤트를 전달했습니다.

하지만, 이 방식은 데이터베이스에 과도하게 의존이 과도하고, 대기열 상태 관리 자체에 DB가 반드시 필요하지 않았으며, 단일 장애점(SPOF)이 될 가능성이 존재했기에 이를 제거하였습니다.

또한, Docker를 활용하여 단일 서버를 분산 환경으로 확장함으로써 가용성을 높혔습니다.<br><br>

### 분산 환경에서 발생한 문제점 및 추가 기능 구현

---

1. 서버별로 독립적인 SSE 연결로 인한 메시지 전달 누락 문제

2. 스케줄링 중복 문제

3. Kafka Consume 재시도 로직 구현<br><br>

### 아키텍처 및 대기열 등록 과정

---

[ 기존의 단일 서버 ]

<p align="center"><img width="625" height="188" alt="Image" src="https://github.com/user-attachments/assets/0205be3b-61b1-4284-a1da-17e9aecd89a4" /><br><br>

😿 단일 서버 리포지토리를 확인해주세요

<br><br>

[ 분산 서버 ]

<p align="center"><img width="900" height="500" alt="Image" src="https://github.com/user-attachments/assets/42426803-62b1-472f-b5cb-9980cb004d61" /><br><br>

**기본 설정**

Docker를 활용하여 분산 환경으로 이루어져 있습니다.

애플리케이션이 시작될 때 각 서버는 이벤트 전달을 위한 Redis 채널을 구독합니다.<br><br>

**분산 서버에서의 대기열 등록 과정**<br>

1. 대기열 등록 요청이 들어오면, 클라이언트는 어떤 대기열인지를 나타내는 queueType, 사용자 ID인 userId, requestKey 값을 서버로 전달합니다.
    
    이때 요청이 도착한 시각을 서버에서 timestamp로 기록하며, 클라이언트는 서버와 SSE 연결을 맺습니다.
    
2. 서버는 먼저 requestKey를 DB에 저장하는 로직을 수행하며, 이때 중복된 requestKey가 존재하면 DuplicateKeyException이 발생하여 중복 요청을 차단하고 queueType, userId, timestamp 정보를 Kafka로 produce 합니다.
3. Kafka 토픽의 메시지를 consume 하면 Redis Sorted Set 자료구조로 이루어진 대기열에 userId를 key로, timestamp를 score로 저장하여, 자동으로 대기열 순서가 정렬되도록 합니다.
4. 이후 Redis Pub/Sub을 통해 다른 서버들에게 대기열 갱신 이벤트를 전파하여, 모든 서버가 동일한 대기열 상태를 공유하도록 합니다.
5. 클라이언트는 SSE 연결을 통해 대기열의 상태가 변할 때마다 대기열 상태를 지속적으로 갱신됩니다.
6. Redis는 대기열과 참가열 두 영역으로 구성되며, 스케줄러가 주기적으로 대기열에서 일정 인원을 참가열로 이동시킵니다. 참가열로 이동된 사용자는 타깃 페이지에 접근할 수 있는 권한을 획득하며 타겟 페이지로 이동하게 됩니다.<br><br>
