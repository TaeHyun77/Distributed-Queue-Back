-- [ Score 생성 + 대기열/참가열 존재 확인 + 등록 진행 중 플래그를 원자적으로 수행하는 Lua 스크립트 ]

-- KEYS[1] = 대기열 키
-- KEYS[2] = 참가열 키
-- KEYS[3] = 밀리초별 카운터 키
-- KEYS[4] = 등록 진행 중 플래그
-- ARGV[1] = userId
-- ARGV[2] = Nginx 타임스탬프 (밀리초, 정수 문자열)

-- 반환값
--   -1  : 대기열에 이미 존재
--   -2  : 참가열에 이미 존재
--   -3  : 등록 진행 중 (Kafka 소비 대기 상태)
--   양수 : 생성된 score (timestampMs * 1000 + seq)

-- 1. 대기열에 이미 존재하는지 확인
if redis.call("ZSCORE", KEYS[1], ARGV[1]) then
    return -1
end

-- 2. 참가열에 이미 존재하는지 확인
if redis.call("ZSCORE", KEYS[2], ARGV[1]) then
    return -2
end

-- 3. 등록 진행 중 여부 확인 ( Kafka 소비 전 동일 사용자 중복 등록 방지 )
local locked = redis.call("SET", KEYS[4], "1", "NX", "EX", 10)
if not locked then
    return -3
end

-- 4. 밀리초별 독립 카운터로 seq 생성 ( 첫 요청 시 3초 TTL 설정, 자동 정리 )
local seq = redis.call("INCR", KEYS[3])
if seq == 1 then
    redis.call("PEXPIRE", KEYS[3], 3000)
end

-- 5. score 계산 후 반환
return tonumber(ARGV[2]) * 1000 + seq
