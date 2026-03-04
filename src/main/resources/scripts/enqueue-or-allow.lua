-- 승격 로직
--
-- KEYS[1] = 참가열 키
-- KEYS[2] = 대기열 키
-- KEYS[3] = 등록 진행 중 플래그

-- ARGV[1] = userId
-- ARGV[2] = maxCapacity (참가열 최대 수용 인원, 정수 문자열)
-- ARGV[3] = nowMs (현재 시각 밀리초, 만료 판단용)
-- ARGV[4] = expireAt (참가열 score, 현재시각 + 10분 밀리초)
-- ARGV[5] = waitScore (대기열 score, generate-score.lua가 생성한 값)
--
-- 반환값
--   1 : 참가열에 직접 삽입 ( 즉시 입장 )
--   0 : 대기열에 삽입 ( 스케줄러 승격 대기 )

-- 1. 참가열에서 만료된 사용자 정리
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[3])

-- 2. 정리 후 참가열 크기 확인
local allowSize = redis.call("ZCARD", KEYS[1])
local maxCap = tonumber(ARGV[2])

-- 3. 참가열에 여유가 있으면 직접 삽입, 없으면 대기열에 삽입
if allowSize < maxCap then
    redis.call("ZADD", KEYS[1], ARGV[4], ARGV[1])
    redis.call("DEL", KEYS[3])
    return 1
else
    redis.call("ZADD", KEYS[2], ARGV[5], ARGV[1])
    redis.call("DEL", KEYS[3])
    return 0
end
