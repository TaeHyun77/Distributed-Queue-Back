---
paths:
  - "src/main/kotlin/com/example/integrated/queue/kafka/**"
---

# Kafka 모듈 규칙

## Consumer

- `@KafkaListener`는 동기 콜백이다. 내부에서 suspend 함수를 호출하려면 `runBlocking` 래핑이 필수이며, 이것은 의도된 예외다.
- 배치 리스너 모드: `isBatchListener = true`, 수동 ACK(`AckMode.MANUAL_IMMEDIATE`)
- 재시도: 최대 3회, 지수 백오프(500/1000/2000ms). 최종 실패 시 DLT(`{topic}-dlt`)로 전송.
- `processWithRetry` 내 `catch (e: Exception)` 블록에서 `CancellationException`을 구분하여 재발생시켜야 한다.

## Producer

- 멱등 프로듀서: `ENABLE_IDEMPOTENCE_CONFIG=true`, `acks=all`
- 압축: lz4, 배치 크기: 64KB, linger: 20ms
- 메시지 키: userId (동일 사용자의 메시지가 같은 파티션으로 보장)
- `kafkaTemplate.send().await()` 사용 시 `CancellationException`을 반드시 재발생시킨다.

## 메시지 형식

`KafkaMessageDto(queueType, userId, timestamp)` — JSON 직렬화/역직렬화에 Jackson `ObjectMapper` 사용.
