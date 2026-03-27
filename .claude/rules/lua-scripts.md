---
paths:
  - "src/main/resources/scripts/**"
---

# Lua 스크립트 수정 규칙

Lua 스크립트는 Redis EVAL로 원자적으로 실행된다. 수정 시 원자성이 깨지지 않도록 주의한다.

## 반환 값 규약

### enqueue-or-allow.lua
- `-1`: 대기열 중복 (이미 대기열에 존재)
- `-2`: 참가열 중복 (이미 참가열에 존재)
- `0`: 대기열 삽입 성공
- `1`: 참가열 직접 삽입 성공 (여유 있을 때)

이 반환 값은 `KafkaConsumerService.handleMessage()`의 `when` 분기에서 사용된다. 반환 값을 변경하면 반드시 호출부도 함께 수정한다.

### schedule-promote.lua
- 반환 값: 승격된 사용자 수 (Long)
- `QueueSchedulerService`에서 호출한다.

## KEYS/ARGV 순서

KEYS와 ARGV의 순서를 변경하면 호출부의 `listOf()` 인자 순서도 반드시 변경한다.

## 카운터 키

밀리초별 시퀀스 카운터 키(`queue:seq:{ms}`)는 5초 TTL로 자동 만료된다. TTL을 변경하면 고부하 시 키 충돌 가능성을 검토한다.
