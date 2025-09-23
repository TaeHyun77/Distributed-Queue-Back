### 프로젝트 설명

---

기존의 예약 시스템과 대기열 시스템을 개선하여 하나로 통합한 프로젝트입니다.

이 리포지토리에서는 개선된 대기열 시스템에 대한 내용을 담고 있습니다.<br><br>

**기존 대기열 시스템에서의 개선점**

기존 대기열 시스템에서는 MySQL과 Kafka Debezium Connector를 활용하여 이벤트를 전달하였으나, 이는 데이터베이스에 지나치게 의존적이었고 특정 지점에서 단일 장애점( SPOF )이 발생할 가능성이 있었습니다. 

또한, 프로젝트가 WebFlux 기반의 비동기 처리 방식을 채택하고 있었기 때문에, 데이터베이스를 거치지 않고 이를 제거하고 직접 Kafka로 이벤트를 발행하는 구조가 더 적합하다고 판단하였습니다.

아울러, 단일 서버 환경에서 분산 서버 환경으로 확장하였고, 분산 환경에서 서버 간의 SSE Sink 데이터를 일관되게 동기화하기 위해 Redis Pub/Sub을 사용하여 사용자의 대기열 상태와 권한 정보가 모든 서버에 반영되도록 개선하였습니다.

결과적으로 개선된 구조는 데이터베이스 의존성을 제거하여 단일 장애점을 제거하고 병목을 줄였으며, 비동기-분산 환경에 적합한 이벤트 전송 및 서버 간 동기화를 가능하게 하였습니다.<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : ~~MySQL ( R2DBC )~~, Redis

Infra : Kafka, Docker<br><br>

### 아키텍처

---

[ 단일 서버 ]

<img width="622" height="232" alt="Image" src="https://github.com/user-attachments/assets/400df547-e600-4093-aa9c-19c2bd2bbccf" />

**단일 서버에서의 대기열 등록 동작 과정**
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이 과정에서 SSE Sink 구독이 이루어집니다.
2. Kafka의 대기열 토픽에 메시지를 발행( Produce )합니다.
3. Kafka Consume을 통해 SSE sink로 전송되며, 사용자의 실시간 순위 및 예약 페이지 접근 권한을 전달받게 됩니다.<br><br>

[ 분산 서버 ]

<img width="1141" height="556" alt="Image" src="https://github.com/user-attachments/assets/1ba96b07-01c6-428a-a8c5-f1eecd641516" /><br><br>

**분산 서버에서의 대기열 등록 동작 과정**

- 모든 서버는 redis의 대기열 채널을 구독한 상태이며, 같은 consumer group으로 설정되어 있습니다.
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이 과정에서 SSE Sink 구독이 이루어집니다.
2. 요청은 Nginx의 로드 밸런싱을 통해 여러 서버 중 하나로 분산되어 전달됩니다.
3. 요청을 받은 서버는 Kafka의 대기열 토픽에 메시지를 발행( Produce )합니다.
    
    ( Produce 시 대기열 이름 값을 key 값으로 전달하여, 동일한 대기열에 속한 사용자는 순서가 보장됩니다. )
    
4. 발행된 메시지는 서버 중 하나에서 소비( Consume )되어 Redis 대기열 채널로 전달됩니다.
5. Redis 대기열 채널에 전달된 메시지는 이를 구독 중인 모든 서버에 전달됩니다.
6. 결과적으로 각 서버의 SSE Sink로 전송되어 동기화가 이루어지며, 사용자의 실시간 순위 및 예약 페이지 접근 권한을 전달받게 됩니다.<br><br>

**예약 동작 과정**

- Redis Mutex Lock을 적용해 동일 좌석 예약 시 단일 요청만 처리되도록 하여 동시성 문제를 방지했습니다.
- 멱등성을 적용해 짧은 시간에 반복된 동일 요청( 따닥 이슈 )을 방지하고, 이미 처리된 요청은 거절하며 기존 결과를 반환하도록 했습니다.

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

<p align="center"><img width="700" height="500" alt="aaa" src="https://github.com/user-attachments/assets/81ac2ac4-b02c-4b40-8f83-3c7c538644e5" />

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
