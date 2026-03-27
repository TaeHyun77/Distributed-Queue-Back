#!/bin/bash
set -euo pipefail

DURATION=10
REDIS="queue-redis-master"
QUEUE="concert"
RESULT_DIR="queue_benchmark_results"
mkdir -p "$RESULT_DIR"

RESULTS_LOG="benchmark_final_results.txt"
> "$RESULTS_LOG"

wait_for_ready() {
    local max_wait=300
    local elapsed=0
    echo "  서비스 기동 대기 중..."
    while true; do
        if docker ps --format "{{.Names}}" 2>/dev/null | grep -q "integrated-nginx"; then
            sleep 3
            echo "  모든 서비스 ready (약 ${elapsed}초)"
            return 0
        fi
        if [ "$elapsed" -ge "$max_wait" ]; then
            echo "  타임아웃 (${max_wait}초)"
            docker ps --format "table {{.Names}}\t{{.Status}}"
            return 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
}

echo ""
echo "======================================================="
echo "  Kafka 제거 후 대기열 벤치마크"
echo "  RPS: 100 ~ 1000 (100 단위)"
echo "  각 RPS마다 compose up/down"
echo "  테스트: ${DURATION}초, 웜업: 100 RPS × ${DURATION}초"
echo "======================================================="

for RPS in 100 200 300 400 500 600 700 800 900 1000; do
    EXPECTED=$((RPS * DURATION))
    echo ""
    echo "══ RPS=${RPS} (${EXPECTED}건/${DURATION}초) ═══════════════════"

    # 1. compose up
    echo "  [1/6] docker-compose up..."
    docker-compose up -d 2>&1 | tail -3

    # 2. 헬스체크 대기
    echo "  [2/6] 헬스체크 대기..."
    wait_for_ready

    # 3. 웜업 (100 RPS)
    echo "  [3/6] 웜업 (100 RPS × ${DURATION}초)..."
    k6 run -e K6_RATE=100 -e K6_DURATION=$DURATION queue-load-test.js > /dev/null 2>&1 || true
    rm -f k6_result.txt

    # 4. 5초 대기 + FLUSHALL
    echo "  [4/6] 5초 대기 + FLUSHALL..."
    sleep 5
    docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1
    sleep 2

    # 5. 본 테스트
    echo "  [5/6] 본 테스트 (${RPS} RPS × ${DURATION}초)..."
    START_TS=$(date +%s)

    k6 run -e K6_RATE=$RPS -e K6_DURATION=$DURATION queue-load-test.js 2>&1

    END_TS=$(date +%s)
    CONSUME=$((END_TS - START_TS))

    # 결과 파싱
    if [ -f k6_result.txt ]; then
        SUCCESS=$(grep "^success=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        FAIL=$(grep "^fail=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        DUP=$(grep "^duplicate=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        HTTP_AVG=$(grep "^http_avg=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        HTTP_P95=$(grep "^http_p95=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        HTTP_MAX=$(grep "^http_max=" k6_result.txt | cut -d= -f2 | tr -d '\r')

        SUCCESS=${SUCCESS:-0}; FAIL=${FAIL:-0}; DUP=${DUP:-0}
        HTTP_AVG=${HTTP_AVG:-0}; HTTP_P95=${HTTP_P95:-0}; HTTP_MAX=${HTTP_MAX:-0}

        TOTAL=$((SUCCESS + FAIL + DUP))
        if [ "$TOTAL" -gt 0 ]; then
            SR=$(awk "BEGIN { printf \"%.1f\", ($SUCCESS / $TOTAL) * 100 }")
        else
            SR="0.0"
        fi

        # Redis 삽입 확인
        W=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:wait" 2>/dev/null | tr -d '\r')
        A=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:allow" 2>/dev/null | tr -d '\r')
        REDIS_TOTAL=$(( ${W:-0} + ${A:-0} ))

        if [ "$CONSUME" -gt 0 ]; then
            THRU=$(awk "BEGIN { printf \"%.1f\", $SUCCESS / $CONSUME }")
        else
            THRU="0.0"
        fi

        echo ""
        echo "  ── 결과 ──"
        echo "  성공=${SUCCESS}  실패=${FAIL}  중복=${DUP}  Redis=${REDIS_TOTAL}"
        echo "  HTTP avg=${HTTP_AVG}ms  p95=${HTTP_P95}ms  max=${HTTP_MAX}ms"
        echo "  소비=${CONSUME}초  처리량=${THRU}/s"

        # 결과 파일 저장
        cat > "$RESULT_DIR/rps_${RPS}.txt" <<EOF
rps=$RPS
success=$SUCCESS
fail=$FAIL
duplicate=$DUP
success_rate=$SR
http_avg=$HTTP_AVG
http_p95=$HTTP_P95
http_max=$HTTP_MAX
consume_sec=$CONSUME
e2e_p95=$HTTP_P95
e2e_max=$HTTP_MAX
throughput=$THRU
EOF

        echo "${RPS}|${SR}%|${HTTP_AVG}|${HTTP_P95}|${CONSUME}s|${HTTP_P95}ms|${HTTP_MAX}ms|${THRU}/s" >> "$RESULTS_LOG"
    else
        echo "  k6_result.txt 없음 — 스킵"
        echo "${RPS}|N/A|N/A|N/A|N/A|N/A|N/A|N/A" >> "$RESULTS_LOG"
    fi

    rm -f k6_result.txt

    # 6. compose down
    echo "  [6/6] compose down..."
    docker-compose down 2>&1 | tail -1
    sleep 5
done

# 최종 결과표
echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Kafka 제거 후 대기열 벤치마크 최종 결과"
echo "════════════════════════════════════════════════════════════════════════════════"
printf "  %-6s | %-8s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
       "RPS" "성공률" "avg(ms)" "p95(ms)" "소비(초)" "E2E p95" "E2E max" "처리량"
echo "────────────────────────────────────────────────────────────────────────────────"

while IFS='|' read -r rps sr avg p95 consume e2e_p95 e2e_max thru; do
    printf "  %-6s | %-8s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
           "$rps" "$sr" "$avg" "$p95" "$consume" "$e2e_p95" "$e2e_max" "$thru"
done < "$RESULTS_LOG"

echo "────────────────────────────────────────────────────────────────────────────────"
echo ""
echo "  * Kafka 제거 후 동기 처리: HTTP 응답 시간 = E2E (비동기 갭 없음)"
echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo ""
echo "BENCHMARK_COMPLETE"
