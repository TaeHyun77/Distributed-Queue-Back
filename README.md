### 프로젝트 설명

---

기존의 단일 서버 기반 대기열 시스템을 분산 환경으로 확장하고, 이에 따른 추가 기능을 구현한 프로젝트입니다.<br><br>

**구현 과정 개인 블로그**<br>

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

하지만, 이 방식은 데이터베이스에 과도하게 의존이 과도했으며, 대기열 상태 관리 자체에 DB가 반드시 필요하지 않았고, DB가 단일 장애점(SPOF)이 될 가능성이 존재했기에 이를 제거하였습니다.

또한, Docker를 활용하여 단일 서버를 분산 환경으로 확장함으로써 가용성을 높혔습니다.<br><br>

### 분산 환경에서 발생한 문제점 및 기능 구현

---

1. 서버 인스턴스별 독립적인 MutableSharedFlow로 인한 SSE 이벤트 전달 누락 문제 💣

2. 분산 환경에서의 스케줄링 중복 실행 문제 💣

3. Kafka Cluster와 Redis Sentinel 기반 Replica 구조를 구성하여 고가용성을 확보하였습니다.

4. Kafka Consumer에서 Redis ZSet 기반 대기열로의 삽입 과정에 재시도 로직을 적용하여, 일시적인 장애 상황에서도 메시지 유실 없이 안정적으로 처리되도록 하였습니다.<br><br>

### 아키텍처 및 대기열 등록 과정

---

[ 기존의 단일 서버 ]

<p align="center"><img width="625" height="188" alt="Image" src="https://github.com/user-attachments/assets/0205be3b-61b1-4284-a1da-17e9aecd89a4" /><br><br>

단일 서버 리포지토리를 확인해주세요

<br><br>

[ 분산 서버 ]

<p align="center"><img width="900" height="500" alt="Image" src="https://github.com/user-attachments/assets/42426803-62b1-472f-b5cb-9980cb004d61" /><br><br>

**기본 설정**

Docker를 활용하여 분산 환경을 구성하였으며, 각 애플리케이션 인스턴스는 시작 시 Redis Pub/Sub 채널을 구독하여 이벤트를 수신할 준비를 합니다.<br><br>

**분산 서버에서의 대기열 등록 과정**<br>

1. 클라이언트는 대기열 등록 요청 시 대기열 식별자인 queueType, 사용자 식별자인 userId, 중복 방지를 위한 requestKey를 서버로 전달합니다.

   서버는 요청 도착 시각을 timestamp로 기록하며, 클라이언트와 SSE 연결을 수립합니다.
   
2. 서버는 requestKey를 DB에 저장하여 중복 요청을 검증합니다. 동일한 requestKey가 이미 존재하면 DuplicateKeyException이 발생하여 중복 요청이 차단되며, 정상 요청인 경우 queueType, userId, timestamp 정보를 Kafka로 produce합니다.

3. Kafka 토픽의 메시지를 consume하면, Redis Sorted Set으로 구성된 대기열에 userId를 member로, timestamp를 score로 저장합니다. 이를 통해 대기열 순서가 요청 시각 기준으로 자동 정렬됩니다.

4. 이후 Redis Pub/Sub을 통해 대기열 갱신 이벤트를 모든 서버 인스턴스에 전파하여, 각 서버가 동일한 대기열 상태를 공유할 수 있도록 합니다.
   
   클라이언트는 SSE 연결을 통해 대기열 상태가 변경될 때마다 실시간으로 갱신된 정보를 수신합니다.

5. 대기열 시스템은 대기열과 참가열 두 영역으로 구성되며, 스케줄러가 주기적으로 대기열에서 일정 인원을 참가열로 승격시킵니다. 참가열로 승격된 사용자는 타겟 페이지에 접근할 수 있는 권한을 획득하며, 클라이언트는 해당 페이지로 이동합니다.

6. 참가열로 승격된 사용자에게는 queueType과 userId 기반의 접근 토큰을 생성하여 클라이언트 쿠키에 저장합니다. 이후 사용자가 타겟 페이지에 진입하면, 서버는 동일한 방식으로 토큰을 재생성하여 클라이언트가 전달한 토큰과 비교하고, 참가열에서의 TTL 만료 여부를 함께 확인하여 사용자의 접근 권한을 검증합니다.
