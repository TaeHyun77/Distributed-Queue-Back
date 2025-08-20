### 프로젝트 설명

---

이 프로젝트는 좌석 예약과 대기열 기능을 통합한 예약 시스템입니다.<br><br>

예약 기능의 동시성 문제를 방지하기 위해 `Redis 기반의 Mutex Lock`과 멱등성 로직을 적용하였고, 대기열 기능에서는 `Redis ZSet`으로 순서를 보장하며 SSE Sink로 실시간 권한 및 순위 변동을 전달하였으며, 기존의 단일 서버 대기열 시스템과 달리, 분산 환경에서의 동작도 함께 고려하였습니다.

분산 환경에서 발생하는 SSE Sink 동기화 문제를 해결하기 위해 Kafka를 통한 안정적인 메시지 전달과 `Redis Pub/Sub`을 활용하여 모든 서버가 동일한 이벤트를 수신, 전달할 수 있도록 하였습니다.<br><br>

### 기술 스택

---

Backend : SpringBoot, Kotlin, Coroutine

Frontend : React.js, JavaScript

Database : MySQL ( R2DBC ), Redis

Infra : Kafka, Docker<br><br>

### 아키텍처

---

<img width="797" height="389" alt="Image" src="https://github.com/user-attachments/assets/239afe7f-3afd-45b2-9340-23d464cf4f3e" /><br><br>

**대기열 등록 동작 과정**

- 모든 서버는 redis의 대기열 채널을 구독한 상태이며, 같은 consumer group으로 설정되어 있습니다.
1. 대기열 등록 요청 시 참가한 대기열의 이름을 전달하며, 이 과정에서 SSE Sink 구독이 이루어집니다.
2. 요청은 Nginx의 로드 밸런싱을 통해 여러 서버 중 하나로 분산되어 전달됩니다.
3. 요청을 받은 서버는 Kafka의 대기열 토픽에 메시지를 발행( Produce )합니다.
    
    ( Produce 시 대기열 이름 값을 key 값으로 전달하여, 동일한 대기열에 속한 사용자는 순서가 보장됩니다. )
    
4. 발행된 메시지는 여러 서버 중 하나에서 소비( Consume )되어 Redis 대기열 채널로 전달됩니다.
5. Redis 대기열 채널에 전달된 메시지는 이를 구독 중인 모든 서버에 전달됩니다.
6. 결과적으로 각 서버의 SSE Sink로 전송되어 동기화가 이루어지며, 사용자의 실시간 순위 및 권한을 전달받게 됩니다.<br><br>

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






