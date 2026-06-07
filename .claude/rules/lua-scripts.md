---
paths:
  - "src/main/resources/scripts/**"
---

# Lua 스크립트 수정 규칙

Lua 스크립트는 Redis EVAL로 원자적으로 실행된다. 수정 시 원자성이 깨지지 않도록 주의한다.

## 반환 값 규약

### enqueue-or-allow.lua
- 반환: `{result, seq}` 배열
  - `result = -1`: 대기열 중복 (이미 대기열에 존재), `seq = 0`
  - `result = -2`: 참가열 중복 (이미 참가열에 존재), `seq = 0`
  - `result =  0`: 대기열 신규 삽입 성공, `seq > 0` (대기열 score 그대로)
  - `result =  1`: 참가열 직접 삽입 성공 (여유 있을 때), `seq = 0`

이 반환 값은 `QueueSchedulerService.enqueueOrAllow()`가 `EnqueueResult(code, seq)`로 받아 `QueueService.registerUserToWaitQueue()`의 `when` 분기에서 사용된다. `result = 0`일 때의 `seq`는 `X-Queue-Seq` 응답 헤더로 클라이언트에 노출된다. 반환 값을 변경하면 반드시 호출부도 함께 수정한다.

### schedule-promote.lua
- 반환 값: 승격된 사용자 수 (Long)
- `QueueSchedulerService`에서 호출한다.

### cancel-user.lua
- `'wait'`  : 대기열에서 제거됨
- `'allow'` : 참가열에서 제거됨
- `'none'`  : 양쪽 모두 없음 (이미 취소/만료/미등록)

이 반환 값은 `QueueService.cancelUser()`에서 `!= "none"`으로 boolean 판정에 사용된다.
위치 구분(`'wait'` vs `'allow'`)은 현재 호출자에서 사용하지 않지만, 향후 확장(예: cancel-from-wait broadcast)을 위해 보존한다.

## KEYS/ARGV 순서

KEYS와 ARGV의 순서를 변경하면 호출부의 `listOf()` 인자 순서도 반드시 변경한다.

## 카운터 키

이벤트별 시퀀스 카운터 키(`queue:seq:{queueType}`)는 영구 보존된다(TTL 없음). `INCR` 결과가 그대로 대기열 ZSet의 score가 되므로 단조 증가가 깨지지 않도록 주의한다. 같은 `queueType`을 회차 단위로 reset하고 싶다면 별도로 `DEL queue:seq:{queueType}`을 명시적으로 호출해야 한다.
