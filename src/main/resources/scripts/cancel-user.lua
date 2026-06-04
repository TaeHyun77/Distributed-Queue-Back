-- 취소 원자적 처리 : wait → allow 순서로 ZREM(remove), 위치 반환
--
-- KEYS[1] : 대기열 키 (ZSet)
-- KEYS[2] : 참가열 키 (ZSet)
--
-- ARGV[1] : userId
--
-- 반환 값:
--   'wait'  : 대기열에서 제거됨
--   'allow' : 참가열에서 제거됨
--   'none'  : 양쪽 모두 없음 (이미 취소/만료/미등록)

local waitKey = KEYS[1]
local allowKey = KEYS[2]
local userId = ARGV[1]

if redis.call('ZREM', waitKey, userId) > 0 then
    return 'wait'
end

if redis.call('ZREM', allowKey, userId) > 0 then
    return 'allow'
end

return 'none'
