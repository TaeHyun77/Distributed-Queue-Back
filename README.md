### 프로젝트 설명

---

기존의 예약 시스템과 대기열 시스템을 개선하여 하나로 통합한 프로젝트입니다.

이 리포지토리에서는 개선된 대기열 시스템에 대한 내용을 담고 있습니다.<br><br>

**구현 과정**

https://velog.io/@ayeah77/series/%EB%8C%80%EA%B8%B0%EC%97%B4-%EC%8B%9C%EC%8A%A4%ED%85%9C-%EA%B0%9C%EC%84%A0<br><br>

**요청 흐름**

대기열 프로젝트는 분산 서버 환경으로 구성되어있습니다.

1. 클라이언트가 서버에 어느 대기열인지를 나타내는 값인 queueType, 사용자의 ID인 userId 정보를 포함하여 대기열 등록 요청을 보내며, 이때 서버는 클라이언트와 SSE 연결을 맺고, 내부적으로 sink 스트림을 구독하여 새 이벤트가 sink에 발행될 때마다 클라이언트에게 데이터를 전송하게 됩니다.

2. 요청이 들어오면 queueType 이름의 ZSet 대기큐에 userId를 key로, 요청 시각을 score로 설정하여 저장합니다.
    
    ZSet은 score를 기준으로 자동 정렬되므로, 사용자는 요청 순서에 따라 대기열에 정렬됩니다.
    
3. 대기열은 대기큐와 허용큐로 구성되며, 사용자가 두 큐 중 어느 곳에도 존재하지 않을 경우에만 대기큐에 등록됩니다.
    
    이후 스케줄러에 의해 일정 인원씩 허용큐로 이동됩니다.
    
4. 사용자의 대기열 등록이 완료되면, 서버는 해당 queueType 정보를 Kafka 토픽에 발행합니다. 모든 서버 인스턴스는 서로 다른 컨슈머 그룹으로 해당 토픽을 구독하고 있기 때문에, 이벤트는 모든 서버로 브로드캐스트됩니다.

5. 각 서버는 Kafka 이벤트를 수신하면 SSE sink를 통해 SSE 이벤트 트리거를 발생시키고, 이를 감지한 서버는 sink를 구독 중인 클라이언트 중 전달된 queueType에 속한 사용자들의 상태 정보를 Redis에서 조회하여 최신 상태를 SSE를 통해 클라이언트로 전송합니다.<br><br>

**기존 대기열 시스템에서의 개선점**

기존 대기열 시스템에서는 MySQL과 Kafka Debezium Connector를 활용하여 이벤트를 전달하였으나, 이는 데이터베이스에 지나치게 의존적이었고 특정 지점에서 단일 장애점( SPOF )이 발생할 가능성이 있었습니다. 

또한, 프로젝트가 WebFlux 기반의 비동기 처리 방식을 채택하고 있었기 때문에, 데이터베이스를 거치지 않고 이를 제거하고 직접 Kafka로 이벤트를 발행하는 구조가 더 적합하다고 판단하였습니다.

시스템을 단일 서버에서 분산 서버 구조로 확장하면서, 서버 간 SSE Sink 데이터의 일관성을 유지하기 위해 기존 Redis Pub/Sub를 사용하였지만 휘발성으로 인한 신뢰성이 떨어지기에, 영속성과 내결함성을 보장하는 Kafka로 변경하였습니다.

결과적으로 개선된 구조는 데이터베이스 의존성을 제거하여 단일 장애점을 제거하고 병목을 줄였으며, 비동기-분산 환경에 적합한 이벤트 전송 및 서버 간 동기화를 가능하게 하였습니다.<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : Redis

Infra : Kafka, Docker

Etc : SSE<br><br>

### 아키텍처

---

[ 단일 서버 ]

<img width="622" height="232" alt="Image" src="https://github.com/user-attachments/assets/400df547-e600-4093-aa9c-19c2bd2bbccf" />

**단일 서버에서의 대기열 등록 동작 과정**
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이 과정에서 SSE Sink 구독이 이루어집니다.
2. Kafka의 대기열 토픽에 메시지를 발행( Produce )합니다.
3. Kafka Consume을 통해 SSE sink로 전송되며, 사용자의 실시간 순위 및 예약 페이지 접근 권한을 전달받게 됩니다.<br><br>

[ 분산 서버 ]

<img width="796" height="441" alt="Image" src="https://github.com/user-attachments/assets/0925969e-fdca-4569-9bb6-b9d5be1ec45a" />
<br><br>

**분산 서버에서의 대기열 등록 동작 과정**

kafka를 pub/sub 기능으로 활용하기 위해 각 서버의 컨슈머가 서로 다른 consumer group으로 되도록 설정하였습니다.<br>

이렇게 한다면 카프카로 발행된 하나의 이벤트를 모든 서버로 전달되게 할 수 있습니다.<br><br>

- 모든 서버의 컨슈머는 서로 다른 consumer group으로 설정되어 있습니다.
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이떄 SSE Sink 구독이 이루어집니다.
2. 요청은 Nginx의 로드 밸런싱을 통해 여러 서버 중 하나로 분산되어 전달됩니다.
3. 요청을 받은 서버는 Kafka의 대기열 토픽에 대기열 이름 값인 queueType을 발행( Produce )합니다.
4. 모든 서버 인스턴스는 서로 다른 컨슈머 그룹으로 해당 토픽을 구독하고 있기 때문에, 이벤트는 모든 서버로 브로드캐스트됩니다.
5. 각 서버는 Kafka 이벤트를 수신하면 SSE sink를 통해 SSE 이벤트 트리거를 발생시키고, 이를 감지한 서버는 sink를 구독 중인 클라이언트 중 전달된 queueType에 속한 사용자들의 상태 정보를 Redis에서 조회하여 최신 상태를 SSE를 통해 클라이언트로 전송합니다.<br><br>

**예약 동작 과정**

- Redis Mutex Lock을 적용해 동일 좌석 예약 시 단일 요청만 처리되도록 하여 동시성 문제를 방지했습니다.
- 멱등성을 적용해 짧은 시간에 반복된 동일 요청( 따닥 이슈 )을 방지하고, 이미 처리된 요청은 거절하며 기존 결과를 반환하도록 했습니다.

- 예약 프로젝트 리포지토리 : https://github.com/TaeHyun77/integrated-reserve-back.git

<br><br>

### **분산 환경에서 확인해야 할 포인트**

---

- [x]  **nginx를 통한 로드 밸런싱이 잘 이루어지는지**
    
    
    여러 요청을 보내봤을 때 요청을 받는 서버 확인
    
    <img width="1477" height="659" alt="Image" src="https://github.com/user-attachments/assets/e975e69d-d9b3-45e2-a9ae-8951d28cc06a" />
    
    ⇒ 요청이 서버에 고르게 로드 밸런싱 되는 모습<br><br>
    
- [x]  **Kafka에서 발행된 메시지가 컨슈머 그룹 내 각 컨슈머 인스턴스에 균등하게 분산되고 있는지 여부**
    
    
    대기열을 처리하는 “queueing-system” 토픽의 파티션 개수를 3개로 증가시킴 ( 더 늘려도 됨 )
    
    <img width="814" height="102" alt="Image" src="https://github.com/user-attachments/assets/8e5add2f-ab7a-4f26-924f-6a91576f259d" /><br><br>
    
    컨슈머 그룹 내의 서버에 파티션 재할당이 잘 되었는지 확인
    
    <img width="1427" height="98" alt="Image" src="https://github.com/user-attachments/assets/28a4036a-abf5-4451-93e2-a7bd4ad8c866" /><br><br>
    
    요청을 보내어 고르게 처리되는지 확인
    
    <img width="1427" height="642" alt="Image" src="https://github.com/user-attachments/assets/75969a3e-7aa3-4dda-8318-2387d9781540" />
      <br><br>
      
### Nginx의 커넥션 관리 및 튜닝
---

<p align="center"><img width="500" height="400" alt="aaa" src="https://github.com/user-attachments/assets/81ac2ac4-b02c-4b40-8f83-3c7c538644e5" />

JMeter로 대기열 요청에 대해 1초 동안 스파이크 테스트를 진행한 결과, 2000건 이상부터 약 3~4%의 오류가 발생했습니다.

Nginx를 거치지 않고 서버에 직접 스파이크 테스트를 진행했을 때는 오류가 발생하지 않음에 따라 Nginx에서의 TCP 연결 관리 과정( 3-way handshake와 4-way handshake 종료 과정 )에서 다량의 TIME_WAIT가 발생하고 이로 인해 오류가 발생한다고 판단했습니다.

오류가 발생한 요청에서는 `NoHttpResponseException`이 발생하였는데, 다음과 같은 원인 때문입니다.

- Nginx가 백엔드 서버의 응답을 받기 전에 연결이 끊어진 경우
- Nginx의 타임아웃 시간 내에 서버가 응답을 반환하지 못한 경우
- Nginx와 백엔드 서버 간 TIME_WAIT 상태가 누적되어 연결 자원이 부족해져 새로운 연결을 할 수 없는 경우
- …

대기열 시스템은 짧은 시간에 대량의 트래픽이 집중되므로 지속적인 요청이 발생하는 환경에서와 달리 커넥션을 유지하여 보관하는 keepalive 설정은 큰 효과를 주지 하며 오히려 자원 낭비로 이어질 수 있습니다. 

대신 워커 프로세스 수를 CPU 코어 수에 따라 조정하면, 스파이크 트래픽 상황에서도 안정적인 성능을 확보할 수 있습니다.

기존에는 워커 프로세스 1개와 각 프로세스당 512개의 요청을 처리하는 설정을 사용하였지만, 이를 CPU 코어 수에 맞춰 워커 프로세스 수를 자동 조정하도록 변경하고, 각 프로세스의 최대 요청 처리 수를 1024로 확장한 결과, 오류가 사라지고 응답 속도가 약 20% 향상되었으며 TPS는 30% 개선되었습니다.
