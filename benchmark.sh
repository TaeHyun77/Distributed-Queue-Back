#!/bin/bash
###############################################
#  E2E Benchmark Script
#  - k6 → Kafka → Consumer → Redis 전체 파이프라인 측정
#
#  Round 1은 JVM 예열 라운드로 취급하여 통계에서 제외
#  Round 2 이후의 결과만으로 성능을 평가
###############################################

ROUNDS=5
WARMUP_ROUNDS=1
QUEUE_KEY="concert:user-queue:wait"
REDIS="queue-redis-master"
CONTAINERS=("queueing01" "queueing02" "queueing03")

EXPECTED_LOAD=3000

TIMEOUT_SECONDS=120
STALL_LIMIT=30

RESULT_DIR="/tmp/bench_results"
mkdir -p "$RESULT_DIR"

declare -a ROUND_TIMES
declare -a ROUND_LOAD_TARGET
declare -a ROUND_CONSUMED

#  --- 유틸 함수 ---

now_rfc3339() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

parse_http_reqs() {
    local k6_file="$1"
    awk '/http_reqs/ && !/phase/ {
        for (i = 1; i <= NF; i++) {
            gsub(/[^0-9]/, "", $i)
            if ($i + 0 >= 1000) { print $i; exit }
        }
    }' "$k6_file"
}

count_e2e_logs() {
    local since="$1"
    local total=0
    for c in "${CONTAINERS[@]}"; do
        local cnt
        cnt=$(docker logs --since "$since" "$c" 2>&1 | grep -c "E2E completed")
        total=$((total + cnt))
    done
    echo "$total"
}

collect_e2e_logs() {
    local since="$1"
    local outfile="$2"
    > "$outfile"
    for c in "${CONTAINERS[@]}"; do
        docker logs --since "$since" "$c" 2>&1 | grep "E2E completed" >> "$outfile"
    done
}

print_e2e_stats() {
    local logfile="$1"
    grep "E2E completed" "$logfile" \
        | sed -n 's/.*duration=\([0-9.]*\)ms.*/\1/p' \
        | sort -n \
        | awk '
            {
                vals[NR] = $1
                sum += $1
            }
            END {
                if (NR == 0) { print "  (데이터 없음)"; exit }
                printf "  count : %d\n", NR
                printf "  min   : %.1f ms\n", vals[1]
                printf "  avg   : %.1f ms\n", sum / NR
                printf "  p50   : %.1f ms\n", vals[int(NR * 0.50) + 1]
                printf "  p95   : %.1f ms\n", vals[int(NR * 0.95) + 1]
                printf "  p99   : %.1f ms\n", vals[int(NR * 0.99) + 1]
                printf "  max   : %.1f ms\n", vals[NR]
            }
        '
}

#  --- 메인 벤치마크 루프 ---

echo "========================================"
echo "  벤치마크 시작 (총 ${ROUNDS}회, 예열 ${WARMUP_ROUNDS}회)"
echo "  요청: ${EXPECTED_LOAD}건/라운드 (300 RPS × 10초)"
echo "  타임아웃: ${TIMEOUT_SECONDS}초 / 정체 감지: ${STALL_LIMIT}초"
echo "========================================"

for ((round=1; round<=ROUNDS; round++)); do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [ "$round" -le "$WARMUP_ROUNDS" ]; then
        echo "  Round $round / $ROUNDS  (JVM 예열)"
    else
        echo "  Round $round / $ROUNDS"
    fi
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── 1. Redis 초기화 ──
    docker exec "$REDIS" redis-cli FLUSHALL > /dev/null 2>&1
    echo "[1/5] Redis FLUSHALL 완료"

    # ── 2. 로그 기준 시각 기록 ──
    ROUND_SINCE=$(now_rfc3339)
    sleep 1
    echo "[2/5] 라운드 시작 시각: $ROUND_SINCE"

    # ── 3. k6 실행 ──
    echo "[3/5] k6 테스트 실행 중..."
    K6_START_EPOCH=$(date +%s%3N)

    k6 run k6test.js --quiet 2>&1 | tee "${RESULT_DIR}/k6_round_${round}.txt"

    K6_END_EPOCH=$(date +%s%3N)
    K6_DURATION_MS=$((K6_END_EPOCH - K6_START_EPOCH))
    echo "  k6 실행 완료: ${K6_DURATION_MS}ms"

    # ── 4. 실제 요청 수 파싱 ──
    LOAD_REQS=$(parse_http_reqs "${RESULT_DIR}/k6_round_${round}.txt")

    if [ -z "$LOAD_REQS" ] || [ "$LOAD_REQS" -eq 0 ] 2>/dev/null; then
        echo "  ⚠ k6 http_reqs 파싱 실패, 기대값(${EXPECTED_LOAD}) 사용"
        LOAD_REQS=$EXPECTED_LOAD
    else
        echo "  k6 실제 요청 수: ${LOAD_REQS}건"
    fi

    ROUND_LOAD_TARGET[$round]=$LOAD_REQS

    # ── 5. Consume 완료 대기 ──
    echo "[4/5] consume 완료 대기 중... (목표: ${LOAD_REQS}건, 타임아웃: ${TIMEOUT_SECONDS}초)"

    WAIT_START=$(date +%s)
    LAST_COUNT=0
    STALL_START=$(date +%s)
    TIMED_OUT=false

    while true; do
        E2E_NOW=$(count_e2e_logs "$ROUND_SINCE")
        ELAPSED=$(( $(date +%s) - WAIT_START ))

        printf "\r  E2E 처리: %d / %d  (경과: %ds)  " "$E2E_NOW" "$LOAD_REQS" "$ELAPSED"

        if [ "$E2E_NOW" -ge "$LOAD_REQS" ]; then
            echo ""
            echo "  ✓ 모든 요청 consume 완료"
            break
        fi

        if [ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]; then
            echo ""
            echo "  ✗ 타임아웃! ${E2E_NOW}/${LOAD_REQS}건 처리됨 (${TIMEOUT_SECONDS}초 초과)"
            TIMED_OUT=true
            break
        fi

        if [ "$E2E_NOW" -ne "$LAST_COUNT" ]; then
            LAST_COUNT="$E2E_NOW"
            STALL_START=$(date +%s)
        elif [ $(( $(date +%s) - STALL_START )) -ge "$STALL_LIMIT" ]; then
            echo ""
            echo "  ⚠ ${STALL_LIMIT}초간 진행 없음 — ${E2E_NOW}/${LOAD_REQS}건으로 확정 (유실 의심: $((LOAD_REQS - E2E_NOW))건)"
            TIMED_OUT=true
            break
        fi

        sleep 2
    done

    CONSUME_END_EPOCH=$(date +%s%3N)
    TOTAL_E2E_MS=$((CONSUME_END_EPOCH - K6_START_EPOCH))
    TOTAL_E2E_SEC=$(awk "BEGIN {printf \"%.1f\", $TOTAL_E2E_MS / 1000}")

    ROUND_CONSUMED[$round]=$E2E_NOW

    if [ "$TIMED_OUT" = true ]; then
        echo "  총 소요 시간 (k6 시작 → 정체 감지): ${TOTAL_E2E_SEC}초"
    else
        echo "  총 소요 시간 (k6 시작 → 전체 consume 완료): ${TOTAL_E2E_SEC}초"
    fi

    # ── 6. 로그 수집 및 통계 ──
    echo ""
    echo "[5/5] 결과 집계"
    E2E_LOG="${RESULT_DIR}/e2e_round_${round}.log"
    collect_e2e_logs "$ROUND_SINCE" "$E2E_LOG"

    E2E_COUNT=$(wc -l < "$E2E_LOG" | tr -d ' ')

    echo ""
    echo "  ┌─── E2E Duration 통계 ───"
    if [ "$E2E_COUNT" -gt 0 ]; then
        print_e2e_stats "$E2E_LOG"
    else
        echo "  │ (E2E 로그 없음)"
    fi
    echo "  └──────────────────────────"

    ZCARD=$(docker exec "$REDIS" redis-cli ZCARD "$QUEUE_KEY" 2>/dev/null)
    echo ""
    echo "  Redis ZCARD ($QUEUE_KEY): ${ZCARD:-N/A}"

    echo ""
    echo "  ┌─── k6 HTTP 요약 ───"
    grep -E "http_req_duration|http_reqs|checks" "${RESULT_DIR}/k6_round_${round}.txt" \
        | head -5 | sed 's/^/  │ /'
    echo "  └─────────────────────"

    ROUND_TIMES[$round]="$TOTAL_E2E_SEC"

    if [ "$round" -lt "$ROUNDS" ]; then
        echo ""
        echo "  다음 라운드까지 5초 대기..."
        sleep 5
    fi
done

###############################################
#  전체 요약
###############################################
echo ""
echo "========================================"
echo "  전체 벤치마크 요약"
echo "  (Round 1은 JVM 예열, Round 2~${ROUNDS} 유효)"
echo "========================================"
echo ""
printf "  %-7s │ %-10s │ %-10s │ %s\n" "Round" "소요시간" "목표(load)" "consume"
echo "  ────────┼────────────┼────────────┼─────────"
for ((r=1; r<=ROUNDS; r++)); do
    TARGET=${ROUND_LOAD_TARGET[$r]:-"?"}
    CONSUMED=${ROUND_CONSUMED[$r]:-"?"}

    if [ "$TARGET" != "?" ] && [ "$CONSUMED" != "?" ]; then
        DIFF=$((TARGET - CONSUMED))
        if [ "$DIFF" -gt 0 ]; then
            STATUS=" ⚠ 유실 ${DIFF}건"
        elif [ "$DIFF" -lt 0 ]; then
            STATUS=" (+$((CONSUMED - TARGET)))"
        else
            STATUS=" ✓"
        fi
    else
        STATUS=""
    fi

    if [ "$r" -le "$WARMUP_ROUNDS" ]; then
        LABEL="(예열)"
    else
        LABEL=""
    fi
    printf "    %d     │  %6s초   │  %5s건    │ %5s건%s %s\n" \
        "$r" "${ROUND_TIMES[$r]}" "$TARGET" "$CONSUMED" "$STATUS" "$LABEL"
done

# 유효 라운드(예열 제외) E2E 통계
echo ""
echo "  ┌─── 유효 라운드 (Round $((WARMUP_ROUNDS+1))~${ROUNDS}) E2E 통계 ───"
> "${RESULT_DIR}/e2e_valid.log"
for ((r=WARMUP_ROUNDS+1; r<=ROUNDS; r++)); do
    cat "${RESULT_DIR}/e2e_round_${r}.log" >> "${RESULT_DIR}/e2e_valid.log" 2>/dev/null
done
VALID_COUNT=$(wc -l < "${RESULT_DIR}/e2e_valid.log" | tr -d ' ')
if [ "$VALID_COUNT" -gt 0 ]; then
    print_e2e_stats "${RESULT_DIR}/e2e_valid.log"
fi
echo "  └────────────────────────────────────"

echo ""
echo "  로그 저장 위치: ${RESULT_DIR}/"
echo "========================================"
echo "  벤치마크 완료"
echo "========================================"