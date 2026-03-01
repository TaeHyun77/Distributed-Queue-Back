#!/bin/bash

ROUNDS=5
QUEUE_KEY="concert:user-queue:wait"
REDIS="queue-redis-master"
CONTAINERS=("queueing01" "queueing02" "queueing03")
STABLE_SECONDS=10  # 10초간 ZCARD 변화 없으면 완료 판단

echo "========================================"
echo "  벤치마크 시작 (총 ${ROUNDS}회)"
echo "========================================"

for ((i=1; i<=ROUNDS; i++)); do
    echo ""
    echo "----------------------------------------"
    echo "  [Round $i / $ROUNDS]"
    echo "----------------------------------------"

    # 1. Redis 초기화
    docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1
    echo "[1/4] Redis 초기화 완료"

    # 2. 라운드 시작 시각 기록 (이전 라운드 로그 필터링용, Unix epoch)
    ROUND_START=$(date +%s)
    sleep 1
    echo "[2/4] 라운드 시작 시각: $ROUND_START"

    # 3. k6 실행
    echo "[3/4] k6 테스트 실행 중..."
    K6_START=$(date +%s)
    k6 run k6test.js --quiet 2>&1 | tee /tmp/k6_round_${i}.txt

    # 4. consume 완료 대기 (k6 요청 수만큼 E2E 로그가 나올 때까지)
    EXPECTED=$(grep "http_reqs\.\." /tmp/k6_round_${i}.txt | head -1 | grep -oE '[0-9]+ +[0-9]+\.' | head -1 | awk '{print $1}')
    echo "[4/4] consume 완료 대기 중... (목표: ${EXPECTED}건)"

    while true; do
        E2E_NOW=0
        for c in "${CONTAINERS[@]}"; do
            CNT=$(docker logs --since "$ROUND_START" "$c" 2>&1 | grep -c "E2E completed")
            E2E_NOW=$((E2E_NOW + CNT))
        done

        echo "  E2E 처리: $E2E_NOW / $EXPECTED"

        if [ "$E2E_NOW" -ge "$EXPECTED" ]; then
            break
        fi

        sleep 2
    done
    CONSUME_END=$(date +%s)
    TOTAL_SEC=$((CONSUME_END - K6_START))
    echo "  E2E 처리 완료: $E2E_NOW건"
    echo "  총 소요 시간 (k6 시작 → 전체 consume 완료): ${TOTAL_SEC}초"

    # 5. 로그 수집 및 집계 (--since로 현재 라운드 로그만 수집)
    echo ""
    echo "  === E2E 결과 ==="
    > /tmp/e2e_round_${i}.log
    for c in "${CONTAINERS[@]}"; do
        docker logs --since "$ROUND_START" "$c" 2>&1 | grep "E2E completed" >> /tmp/e2e_round_${i}.log
    done

    E2E_COUNT=$(wc -l < /tmp/e2e_round_${i}.log | tr -d ' ')
    echo "  E2E 로그 수: $E2E_COUNT"

    if [ "$E2E_COUNT" -gt 0 ]; then
        grep "E2E completed" /tmp/e2e_round_${i}.log | awk -F'duration=' '{print $2}' | awk -F'ms' '{print $1}' | sort -n | awk '
            { vals[NR]=$1; sum+=$1 }
            END {
                printf "  count: %d\n", NR
                printf "  avg:   %.1f ms\n", sum/NR
                printf "  p50:   %.1f ms\n", vals[int(NR*0.5)]
                printf "  p95:   %.1f ms\n", vals[int(NR*0.95)]
                printf "  p99:   %.1f ms\n", vals[int(NR*0.99)]
                printf "  max:   %.1f ms\n", vals[NR]
            }
        '
    fi

    echo ""
    echo "  === k6 요약 ==="
    grep -E "http_req_duration|http_reqs" /tmp/k6_round_${i}.txt | head -3
done

echo ""
echo "========================================"
echo "  전체 벤치마크 완료"
echo "========================================"
