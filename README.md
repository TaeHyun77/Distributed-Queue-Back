### 프로젝트 설명

---

이번 프로젝트는 좌석 예약과 대기열 기능을 통합한 예약 시스템입니다.<br><br>

본 프로젝트에서는 예약 기능의 동시성 문제를 방지하기 위해 `Redis 기반의 Mutex Lock`과 멱등성 로직을 적용하였고, 대기열 기능에서는 `Redis ZSet`으로 순서를 보장하며 SSE Sink로 실시간 권한 및 순위 변동을 전달하였으며, 분산 환경에서 발생하는 SSE Sink 동기화 문제를 해결하기 위해 Kafka를 통한 안정적인 메시지 전달과 `Redis Pub/Sub`을 활용하여 모든 서버가 동일한 이벤트를 수신, 전달할 수 있도록 구현하였습니다.
<br><br>
### 기술 스택

---

Backend : SpringBoot, Java, Kotlin

Frontend : React.js, JavaScript

Database : MySQL, Redis

Infra : Kafka<br><br>

### 아키텍처

---

![image.png](attachment:7c3cf070-10c5-4c67-8637-ce3c9a2ccd37:6ab52c0c-1a11-424c-9955-899a7676e4a1.png)

<br><br>

### **분산 환경에서 확인해야 할 포인트**

---

- [x]  **nginx를 통한 로드 밸런싱이 잘 이루어지는지**
    
    
    여러 요청을 보내봤을 때 요청을 받는 서버 확인
    
    ![image.png](attachment:4ef02bc0-98da-445d-a078-4dde1dbc1653:image.png)
    
    ⇒ 요청이 서버에 고르게 로드 밸런싱 되는 모습<br><br>
    
- [x]  **Kafka에서 발행된 메시지가 컨슈머 그룹 내 각 컨슈머 인스턴스에 균등하게 분산되고 있는지 여부**
    
    
    대기열을 처리하는 “queueing-system” 토픽의 파티션 개수를 3개로 증가시킴 ( 더 늘려도 됨 )
    
    ![image.png](attachment:2179f3a6-b587-45fb-bfd3-dae9ccf40be3:image.png)
    
    컨슈머 그룹 내의 서버에 파티션 재할당이 잘 되었는지 확인
    
    ![image.png](attachment:e630d44f-9dc4-4c4a-bbc2-828344ff4f5e:image.png)
    
    요청을 보내어 고르게 처리되는지 확인
    
    ![image.png](attachment:90fd6539-e29e-4de7-a80b-42d47751cae6:image.png)
