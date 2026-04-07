#!/bin/bash
# 대기열 부하 테스트 벤치마크
#
# RPS를 단계적으로 올리며 대기열 시스템의 처리 한계를 측정합니다.
#
# 측정 항목:
#   1. HTTP 응답 시간 (= E2E) — enqueue-or-allow.lua (원자적 삽입) + Pub/Sub 알림
#   2. 전체 소비 완료 시간 — 첫 요청 → 마지막 Redis 삽입 확인
#
# 테스트 흐름:
#   1. 웜업 (50명 등록 → 20초 안정화 → FLUSHALL)
#   2. RPS별 2회 반복 테스트 (FLUSHALL → 본 테스트 → 삽입 확인 → 결과 수집)
#   3. 2회 평균 결과 비교표 출력
#
# 사용법:
#   ./queue-benchmark-test.sh                     → 기본 (RPS: 100 300 500 700 1000)
#   ./queue-benchmark-test.sh "100 200 300 400"   → 커스텀 RPS 단계
#
# 사전 조건:
#   - docker-compose up 상태

set -euo pipefail

# ---- 설정 ----
RPS_LEVELS=(${1:-100 300 500 700 1000})
DURATION=10
RUNS=2
WARMUP_RPS=50
WARMUP_DURATION=1
WARMUP_STABILIZE=20
RESET_WAIT=2

REDIS="queue-redis-master"
APPS=("queueing01" "queueing02" "queueing03")
QUEUE="concert"

echo ""
echo "======================================================="
echo "  대기열 부하 테스트 — 처리 한계 측정"
echo "  RPS 단계: ${RPS_LEVELS[*]}"
echo "  각 ${DURATION}초 × ${RUNS}회 반복"
echo "======================================================="

# ---- 1. 웜업 ----
echo ""
echo "[1/3] JVM 웜업 — ${WARMUP_RPS}명 등록 → ${WARMUP_STABILIZE}초 안정화 → FLUSHALL"

k6 run -e K6_RATE=$WARMUP_RPS -e K6_DURATION=$WARMUP_DURATION queue-load-test.js > /dev/null 2>&1 || true
rm -f k6_result.txt

echo "  ${WARMUP_STABILIZE}초 안정화 대기 중... (JIT 완료)"
sleep $WARMUP_STABILIZE

docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1
sleep $RESET_WAIT
echo "  웜업 완료 + FLUSHALL"

# ---- E2E 통계 계산 함수 ----
calc_e2e_stats() {
    local e2e_data="$1"
    echo "$e2e_data" | tr ' ' '\n' | grep -v '^$' | sort -n | awk '
    {
        vals[NR] = $1
        sum += $1
        c++
        if (c == 1 || $1 + 0 < mn + 0) mn = $1
        if (c == 1 || $1 + 0 > mx + 0) mx = $1
    }
    END {
        if (c > 0) {
            avg = sum / c
            p95_idx = int(c * 0.95)
            if (p95_idx < 1) p95_idx = 1
            printf "%d %.2f %d %d", mn, avg, mx, vals[p95_idx]
        } else {
            printf "0 0 0 0"
        }
    }'
}

# ---- 2. RPS별 테스트 (2회 반복) ----
echo ""
echo "[2/3] RPS별 테스트 시작..."

RESULT_DIR="queue_benchmark_results"
mkdir -p "$RESULT_DIR"

for RATE in "${RPS_LEVELS[@]}"; do
    EXPECTED=$((RATE * DURATION))
    POLL_TIMEOUT=$((60 + EXPECTED / 100))

    echo ""
    echo "  ══ RPS=${RATE} (${EXPECTED}건 / ${DURATION}초) × ${RUNS}회 ══"

    # 회차별 결과 저장
    declare -a R_SUCCESS R_FAIL R_DUP R_HTTP_AVG R_HTTP_P95 R_HTTP_P99 R_HTTP_MAX
    declare -a R_CONSUME R_E2E_MIN R_E2E_AVG R_E2E_MAX R_E2E_P95

    for ((run=0; run<RUNS; run++)); do
        RUN_NUM=$((run + 1))
        echo ""
        echo "    [${RUN_NUM}/${RUNS}회차]"

        # Redis 초기화
        docker exec $REDIS redis-cli FLUSHALL > /dev/null 2>&1
        sleep $RESET_WAIT

        # 시작 시각
        START=$(date +%s)

        # k6 실행
        k6 run -e K6_RATE=$RATE -e K6_DURATION=$DURATION queue-load-test.js

        # k6 결과 파싱
        if [ ! -f k6_result.txt ]; then
            echo "      k6_result.txt 없음, 건너뜁니다"
            R_SUCCESS[$run]=0; R_FAIL[$run]=0; R_DUP[$run]=0
            R_HTTP_AVG[$run]=0; R_HTTP_P95[$run]=0; R_HTTP_P99[$run]=0; R_HTTP_MAX[$run]=0
            R_CONSUME[$run]=0; R_E2E_MIN[$run]=0; R_E2E_AVG[$run]=0; R_E2E_MAX[$run]=0; R_E2E_P95[$run]=0
            continue
        fi

        SUCCESS=$(grep "^success=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_SUCCESS[$run]=${SUCCESS:-0}
        R_FAIL[$run]=$(grep "^fail=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_DUP[$run]=$(grep "^duplicate=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_HTTP_AVG[$run]=$(grep "^http_avg=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_HTTP_P95[$run]=$(grep "^http_p95=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_HTTP_P99[$run]=$(grep "^http_p99=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_HTTP_MAX[$run]=$(grep "^http_max=" k6_result.txt | cut -d= -f2 | tr -d '\r')

        REGISTERED=${SUCCESS:-0}

        # 성공 0건이면 스킵
        if [ "$REGISTERED" -eq 0 ] 2>/dev/null; then
            echo "      성공 0건, 건너뜁니다"
            R_CONSUME[$run]=0; R_E2E_MIN[$run]=0; R_E2E_AVG[$run]=0; R_E2E_MAX[$run]=0; R_E2E_P95[$run]=0
            rm -f k6_result.txt
            continue
        fi

        # Redis 삽입 확인
        echo "      Redis 삽입 확인 중..."
        POLL_START=$(date +%s)
        TOTAL=0
        while true; do
            W=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:wait" 2>/dev/null | tr -d '\r')
            A=$(docker exec $REDIS redis-cli ZCARD "${QUEUE}:user-queue:allow" 2>/dev/null | tr -d '\r')
            TOTAL=$(( ${W:-0} + ${A:-0} ))

            [ "$TOTAL" -ge "$REGISTERED" ] && break

            NOW=$(date +%s)
            if [ $((NOW - POLL_START)) -ge $POLL_TIMEOUT ]; then
                echo "      타임아웃 (${POLL_TIMEOUT}초), 현재 ${TOTAL}/${REGISTERED}건"
                break
            fi

            sleep 0.5
        done

        END=$(date +%s)
        R_CONSUME[$run]=$((END - START))

        echo "      소비 완료: ${TOTAL}/${REGISTERED}건 (${R_CONSUME[$run]}초)"

        # E2E = HTTP (Kafka 제거 후 동기 처리이므로 HTTP 응답 시간 = E2E)
        HTTP_MIN=$(grep "^http_min=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_E2E_MIN[$run]=${HTTP_MIN:-0}
        R_E2E_AVG[$run]=${R_HTTP_AVG[$run]}
        R_E2E_MAX[$run]=$(grep "^http_max=" k6_result.txt | cut -d= -f2 | tr -d '\r')
        R_E2E_P95[$run]=${R_HTTP_P95[$run]}

        echo "      E2E = HTTP (동기 처리): avg=${R_E2E_AVG[$run]}ms  p95=${R_E2E_P95[$run]}ms  max=${R_E2E_MAX[$run]}ms"

        rm -f k6_result.txt

        # 회차 간 안정화 대기
        if [ $run -lt $((RUNS - 1)) ]; then
            echo "      다음 회차 전 안정화 대기 (30초)..."
            sleep 30
        fi
    done

    # 2회 평균 계산
    avg2() { awk "BEGIN { printf \"$1\", ($2 + $3) / 2 }"; }

    AVG_SUCCESS=$(avg2 "%.0f" "${R_SUCCESS[0]}" "${R_SUCCESS[1]}")
    AVG_HTTP_AVG=$(avg2 "%.2f" "${R_HTTP_AVG[0]}" "${R_HTTP_AVG[1]}")
    AVG_HTTP_P95=$(avg2 "%.2f" "${R_HTTP_P95[0]}" "${R_HTTP_P95[1]}")
    AVG_HTTP_P99=$(avg2 "%.2f" "${R_HTTP_P99[0]}" "${R_HTTP_P99[1]}")
    AVG_HTTP_MAX=$(avg2 "%.2f" "${R_HTTP_MAX[0]}" "${R_HTTP_MAX[1]}")
    AVG_CONSUME=$(avg2 "%.1f" "${R_CONSUME[0]}" "${R_CONSUME[1]}")
    AVG_E2E_AVG=$(avg2 "%.2f" "${R_E2E_AVG[0]}" "${R_E2E_AVG[1]}")
    AVG_E2E_P95=$(avg2 "%.0f" "${R_E2E_P95[0]}" "${R_E2E_P95[1]}")
    AVG_E2E_MAX=$(avg2 "%.0f" "${R_E2E_MAX[0]}" "${R_E2E_MAX[1]}")
    AVG_FAIL=$(avg2 "%.0f" "${R_FAIL[0]}" "${R_FAIL[1]}")
    AVG_DUP=$(avg2 "%.0f" "${R_DUP[0]}" "${R_DUP[1]}")
    THROUGHPUT=$(awk "BEGIN { if($AVG_CONSUME>0) printf \"%.1f\", $AVG_SUCCESS / $AVG_CONSUME; else print \"0\" }")

    TOTAL_AVG=$(awk "BEGIN { printf \"%.0f\", $AVG_SUCCESS + $AVG_FAIL + $AVG_DUP }")
    if [ "$TOTAL_AVG" -gt 0 ] 2>/dev/null; then
        SR=$(awk "BEGIN { printf \"%.1f\", ($AVG_SUCCESS / $TOTAL_AVG) * 100 }")
    else
        SR="0.0"
    fi

    # 결과 파일 저장 (2회 평균)
    cat > "$RESULT_DIR/rps_${RATE}.txt" <<RESULT_EOF
rps=$RATE
success=$AVG_SUCCESS
fail=$AVG_FAIL
duplicate=$AVG_DUP
success_rate=$SR
http_avg=$AVG_HTTP_AVG
http_p95=$AVG_HTTP_P95
http_p99=$AVG_HTTP_P99
http_max=$AVG_HTTP_MAX
consume_sec=$AVG_CONSUME
e2e_avg=$AVG_E2E_AVG
e2e_p95=$AVG_E2E_P95
e2e_max=$AVG_E2E_MAX
throughput=$THROUGHPUT
RESULT_EOF

    echo ""
    echo "    [평균] 성공=${AVG_SUCCESS}  HTTP avg=${AVG_HTTP_AVG}ms  p95=${AVG_HTTP_P95}ms  소비=${AVG_CONSUME}초  E2E p95=${AVG_E2E_P95}ms"
done

# ---- 3. 결과 비교표 ----
echo ""
echo "[3/3] 최종 결과"

L="=============================================================================================="
D="----------------------------------------------------------------------------------------------"

echo ""
echo "$L"
echo "  대기열 부하 테스트 — 처리 한계 측정 결과 (${RUNS}회 평균)"
echo "$L"
printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
       "RPS" "성공률" "avg(ms)" "p95(ms)" "소비(초)" "E2E p95" "E2E max" "처리량"
echo "$D"

RECOMMENDED=""
P95_THRESHOLD=1000

for RATE in "${RPS_LEVELS[@]}"; do
    FILE="$RESULT_DIR/rps_${RATE}.txt"

    if [ ! -f "$FILE" ]; then
        printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
               "$RATE" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A"
        continue
    fi

    SR=$(grep "^success_rate=" "$FILE" | cut -d= -f2 | tr -d '\r')
    HTTP_AVG=$(grep "^http_avg=" "$FILE" | cut -d= -f2 | tr -d '\r')
    HTTP_P95=$(grep "^http_p95=" "$FILE" | cut -d= -f2 | tr -d '\r')
    CONSUME=$(grep "^consume_sec=" "$FILE" | cut -d= -f2 | tr -d '\r')
    E2E_P95=$(grep "^e2e_p95=" "$FILE" | cut -d= -f2 | tr -d '\r')
    E2E_MAX=$(grep "^e2e_max=" "$FILE" | cut -d= -f2 | tr -d '\r')
    THRU=$(grep "^throughput=" "$FILE" | cut -d= -f2 | tr -d '\r')

    printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
           "$RATE" "${SR:-0}%" "${HTTP_AVG:-N/A}" "${HTTP_P95:-N/A}" \
           "${CONSUME:-N/A}s" "${E2E_P95:-N/A}ms" "${E2E_MAX:-N/A}ms" "${THRU:-N/A}/s"

    # p95 기준 권장 RPS 산정
    if [ -n "$HTTP_P95" ]; then
        IS_UNDER=$(awk "BEGIN { print (${HTTP_P95} < ${P95_THRESHOLD}) ? 1 : 0 }")
        if [ "$IS_UNDER" = "1" ]; then
            RECOMMENDED=$RATE
        fi
    fi
done

echo "$D"
echo ""

if [ -n "$RECOMMENDED" ]; then
    echo "  안정적 처리 가능 RPS: ${RECOMMENDED}"
    echo "  (기준: HTTP p95 < ${P95_THRESHOLD}ms)"
else
    echo "  모든 RPS에서 p95 > ${P95_THRESHOLD}ms — 더 낮은 RPS 테스트 필요"
fi

echo ""
echo "$L"
echo ""
echo "  결과 파일: $(pwd)/$RESULT_DIR/"
echo ""