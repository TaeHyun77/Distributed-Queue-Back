#!/bin/bash
###############################################
#  E2E Benchmark Script
#  - k6 → Kafka → Consumer → Redis 전체 파이프라인 측정
#
#  E2E Duration: 개별 요청이 HTTP 진입 → Kafka produce → consume → Redis write
#                까지 걸리는 시간 (애플리케이션 로그의 "E2E completed" 기준)
#  소요시간: load_test 시작 → 모든 요청 consume 완료까지의 wall clock 시간
###############################################

ROUNDS=5
QUEUE_KEY="concert:user-queue:wait"
REDIS="queue-redis-master"
CONTAINERS=("queueing01" "queueing02" "queueing03")

# k6 시나리오 설정값
EXPECTED_WARMUP=200
EXPECTED_LOAD=3000
EXPECTED_TOTAL=$((EXPECTED_LOAD + EXPECTED_WARMUP))

# consume 완료 대기 최대 시간 (초)
TIMEOUT_SECONDS=120
# 진행 정체 판단 기준 (초) — 이 시간 동안 변화 없으면 종료
STALL_LIMIT=30

# 결과 저장 디렉토리
RESULT_DIR="/tmp/bench_results"
mkdir -p "$RESULT_DIR"

# 최종 요약용 배열
declare -a ROUND_TIMES
declare -a ROUND_PRODUCED
declare -a ROUND_CONSUMED
declare -a ROUND_LOAD_TARGET

#  --- 유틸 함수 ---

now_rfc3339() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

# k6 출력에서 전체 http_reqs 수 파싱
parse_http_reqs() {
    local k6_file="$1"
    awk '/http_reqs/ && !/phase/ {
        for (i = 1; i <= NF; i++) {
            gsub(/[^0-9]/, "", $i)
            if ($i + 0 >= 1000) { print $i; exit }
        }
    }' "$k6_file"
}

# k6 출력에서 phase:test 실제 건수 파싱
# http_req_duration의 phase:test 서브라인에서 count=N 추출
# 예: ✓ { phase:test }...: avg=19.27ms ... count=3001
parse_load_reqs() {
    local k6_file="$1"
    awk '/phase:test/ && /count=/ {
        match($0, /count=[0-9]+/)
        if (RSTART > 0) {
            print substr($0, RSTART + 6, RLENGTH - 6)
            exit
        }
    }' "$k6_file"
}

# 모든 컨테이너에서 특정 시점 이후 "E2E completed" 로그 수 합산
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

# 모든 컨테이너에서 E2E 로그를 파일로 수집
collect_e2e_logs() {
    local since="$1"
    local outfile="$2"
    > "$outfile"
    for c in "${CONTAINERS[@]}"; do
        docker logs --since "$since" "$c" 2>&1 | grep "E2E completed" >> "$outfile"
    done
}

# E2E duration 통계 계산
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
echo "  벤치마크 시작 (총 ${ROUNDS}회)"
echo "  요청: warmup ${EXPECTED_WARMUP} + load ${EXPECTED_LOAD} = ${EXPECTED_TOTAL}건"
echo "  타임아웃: ${TIMEOUT_SECONDS}초 / 정체 감지: ${STALL_LIMIT}초"
echo "========================================"

for ((round=1; round<=ROUNDS; round++)); do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Round $round / $ROUNDS"
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
    ACTUAL_REQS=$(parse_http_reqs "${RESULT_DIR}/k6_round_${round}.txt")

    if [ -z "$ACTUAL_REQS" ] || [ "$ACTUAL_REQS" -eq 0 ] 2>/dev/null; then
        echo "  ⚠ k6 http_reqs 파싱 실패, 기대값(${EXPECTED_TOTAL}) 사용"
        ACTUAL_REQS=$EXPECTED_TOTAL
    else
        echo "  k6 실제 요청 수: ${ACTUAL_REQS}건"
    fi

    # phase:test 실제 건수 파싱 (warmup 제외된 정확한 load 수)
    LOAD_REQS=$(parse_load_reqs "${RESULT_DIR}/k6_round_${round}.txt")
    if [ -z "$LOAD_REQS" ] || [ "$LOAD_REQS" -eq 0 ] 2>/dev/null; then
        LOAD_REQS=$((ACTUAL_REQS - EXPECTED_WARMUP))
        echo "  ⚠ phase:test 파싱 실패, 추정값 사용: ${LOAD_REQS}건"
    else
        echo "  load_test 실제 요청: ${LOAD_REQS}건"
    fi

    ROUND_PRODUCED[$round]=$ACTUAL_REQS
    ROUND_LOAD_TARGET[$round]=$LOAD_REQS

    # load_test 시작 시각: warmup(5s) + startTime gap(2s) = k6 시작 후 7초
    LOAD_START_EPOCH=$((K6_START_EPOCH + 7000))
    LOAD_START_SINCE=$(date -u -d @$((LOAD_START_EPOCH / 1000)) +"%Y-%m-%dT%H:%M:%SZ")

    # ── 5. Consume 완료 대기 ──
    echo "[4/5] consume 완료 대기 중... (목표: ${LOAD_REQS}건, 타임아웃: ${TIMEOUT_SECONDS}초)"

    WAIT_START=$(date +%s)
    LAST_COUNT=0
    STALL_START=$(date +%s)
    TIMED_OUT=false

    while true; do
        E2E_NOW=$(count_e2e_logs "$LOAD_START_SINCE")
        ELAPSED=$(( $(date +%s) - WAIT_START ))

        printf "\r  E2E 처리: %d / %d  (경과: %ds)  " "$E2E_NOW" "$LOAD_REQS" "$ELAPSED"

        # 목표 달성
        if [ "$E2E_NOW" -ge "$LOAD_REQS" ]; then
            echo ""
            echo "  ✓ 모든 요청 consume 완료"
            break
        fi

        # 타임아웃 체크
        if [ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]; then
            echo ""
            echo "  ✗ 타임아웃! ${E2E_NOW}/${LOAD_REQS}건 처리됨 (${TIMEOUT_SECONDS}초 초과)"
            TIMED_OUT=true
            break
        fi

        # 진행 정체 감지 — STALL_LIMIT 초간 변화 없으면 종료
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
    TOTAL_E2E_MS=$((CONSUME_END_EPOCH - LOAD_START_EPOCH))
    TOTAL_E2E_SEC=$(awk "BEGIN {printf \"%.1f\", $TOTAL_E2E_MS / 1000}")

    ROUND_CONSUMED[$round]=$E2E_NOW

    if [ "$TIMED_OUT" = true ]; then
        echo "  총 소요 시간 (load_test 시작 → 정체 감지): ${TOTAL_E2E_SEC}초"
    else
        echo "  총 소요 시간 (load_test 시작 → 전체 consume 완료): ${TOTAL_E2E_SEC}초"
    fi

    # ── 6. 로그 수집 및 통계 ──
    echo ""
    echo "[5/5] 결과 집계"
    E2E_LOG="${RESULT_DIR}/e2e_round_${round}.log"
    collect_e2e_logs "$LOAD_START_SINCE" "$E2E_LOG"

    E2E_COUNT=$(wc -l < "$E2E_LOG" | tr -d ' ')

    echo ""
    echo "  ┌─── E2E Duration 통계 ───"
    if [ "$E2E_COUNT" -gt 0 ]; then
        print_e2e_stats "$E2E_LOG"
    else
        echo "  │ (E2E 로그 없음)"
    fi
    echo "  └──────────────────────────"

    # Redis 최종 상태
    ZCARD=$(docker exec "$REDIS" redis-cli ZCARD "$QUEUE_KEY" 2>/dev/null)
    echo ""
    echo "  Redis ZCARD ($QUEUE_KEY): ${ZCARD:-N/A}"

    # k6 HTTP 요약
    echo ""
    echo "  ┌─── k6 HTTP 요약 ───"
    grep -E "http_req_duration|http_reqs|checks" "${RESULT_DIR}/k6_round_${round}.txt" \
        | head -5 | sed 's/^/  │ /'
    echo "  └─────────────────────"

    # 라운드 결과 저장
    ROUND_TIMES[$round]="$TOTAL_E2E_SEC"

    # 다음 라운드 대기
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
    printf "    %d     │  %6s초   │  %5s건    │ %5s건%s\n" \
        "$r" "${ROUND_TIMES[$r]}" "$TARGET" "$CONSUMED" "$STATUS"
done

# 전체 E2E 통계
echo ""
echo "  ┌─── 전체 라운드 합산 E2E 통계 ───"
cat "${RESULT_DIR}"/e2e_round_*.log > "${RESULT_DIR}/e2e_all.log"
ALL_COUNT=$(wc -l < "${RESULT_DIR}/e2e_all.log" | tr -d ' ')
if [ "$ALL_COUNT" -gt 0 ]; then
    print_e2e_stats "${RESULT_DIR}/e2e_all.log"
fi
echo "  └────────────────────────────────────"

echo ""
echo "  로그 저장 위치: ${RESULT_DIR}/"
echo "========================================"
echo "  벤치마크 완료"
echo "========================================"
