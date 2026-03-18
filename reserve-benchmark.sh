#!/bin/bash
# 예약 부하 테스트 벤치마크 — max-capacity 산정
#
# 동시 사용자 수를 단계적으로 증가시키며 테스트하여
# 대기열 시스템의 최적 max-capacity 값을 산정합니다.
#
# 테스트 흐름:
#   1. 테스트 데이터 생성 (500명 유저, 5000석)
#   2. JVM 웜업 (10명 예약 → 데이터 초기화)
#   3. 각 VU 단계별 버스트 테스트 (테스트 → 데이터 초기화 반복)
#   4. 결과 비교표 출력 + 권장 max-capacity 산정
#
# Docker 재시작 불필요:
#   JIT 웜업 상태는 프로덕션과 동일한 조건.
#   데이터 초기화(/reserve/init/reset)만으로 테스트 간 독립성 보장.
#
# 사용법:
#   ./reserve-benchmark.sh                       → 기본 (VU: 50 100 200 300 500)
#   ./reserve-benchmark.sh "50 100 150 200 250"  → 커스텀 VU 단계
#   SCHEDULE_ID=10 ./reserve-benchmark.sh        → scheduleId 직접 지정
#
# 사전 조건:
#   - reserve 시스템 docker-compose up 상태
#   - k6 설치 완료

set -euo pipefail

# ---- 설정 ----
VU_LEVELS=(${1:-50 100 200 300 500})
BASE_URL=${BASE_URL:-"http://localhost:8079"}
WARMUP_VUS=${WARMUP_VUS:-50}
RESET_WAIT=3
WARMUP_STABILIZE=${WARMUP_STABILIZE:-10}
P95_THRESHOLD=500  # 권장 max-capacity 산정 기준 (p95 < 이 값)

echo ""
echo "======================================================="
echo "  예약 부하 테스트 — max-capacity 산정"
echo "  VU 단계: ${VU_LEVELS[*]}"
echo "  p95 기준 임계값: ${P95_THRESHOLD}ms"
echo "======================================================="

# ---- 1. 테스트 데이터 생성 ----
echo ""
echo "[1/4] 테스트 데이터 생성 중..."
INIT_RES=$(curl -s -X POST "$BASE_URL/reserve/init/load-test")
echo "  응답: $INIT_RES"

# SCHEDULE_ID가 외부에서 지정되지 않은 경우 init 응답에서 추출
if [ -z "${SCHEDULE_ID:-}" ]; then
    if command -v jq &> /dev/null; then
        SCHEDULE_ID=$(echo "$INIT_RES" | jq -r '.scheduleId')
    else
        SCHEDULE_ID=$(echo "$INIT_RES" | sed -n 's/.*"scheduleId":\([0-9]*\).*/\1/p')
    fi
fi

if [ -z "$SCHEDULE_ID" ] || [ "$SCHEDULE_ID" = "null" ]; then
    echo "  오류: scheduleId를 추출할 수 없습니다."
    echo "  수동 지정: SCHEDULE_ID=10 ./reserve-benchmark.sh"
    exit 1
fi

echo "  scheduleId: $SCHEDULE_ID"

# ---- 2. JVM 웜업 ----
if [ "$WARMUP_VUS" -gt 0 ]; then
    echo ""
    echo "[2/4] JVM 웜업 - ${WARMUP_VUS}명 실제 예약 → 데이터 초기화"

    k6 run \
        -e VUS=$WARMUP_VUS \
        -e SCHEDULE_ID=$SCHEDULE_ID \
        -e BASE_URL=$BASE_URL \
        reserve-load-test.js > /dev/null 2>&1 || true

    rm -f reserve_result.txt

    # 웜업 데이터 초기화 + 안정화 대기
    curl -s -X POST "$BASE_URL/reserve/init/reset?scheduleId=$SCHEDULE_ID" > /dev/null
    echo "  웜업 완료, ${WARMUP_STABILIZE}초 안정화 대기 중..."
    sleep $WARMUP_STABILIZE
    echo "  안정화 완료 → 테스트 시작"
else
    echo ""
    echo "[2/4] JVM 웜업 건너뜀 (WARMUP_VUS=0)"
fi

echo "  웜업 완료 + 데이터 초기화"

# ---- 3. 단계별 버스트 테스트 ----
echo ""
echo "[3/4] 단계별 버스트 테스트 시작..."

RESULT_DIR="reserve_benchmark_results"
mkdir -p "$RESULT_DIR"

for VUS in "${VU_LEVELS[@]}"; do
    echo ""
    echo "  ── VU=${VUS} 테스트 ──"

    k6 run \
        -e VUS=$VUS \
        -e SCHEDULE_ID=$SCHEDULE_ID \
        -e BASE_URL=$BASE_URL \
        reserve-load-test.js

    if [ -f reserve_result.txt ]; then
        mv reserve_result.txt "$RESULT_DIR/vu_${VUS}.txt"
    else
        echo "  경고: 결과 파일 없음 (VU=$VUS)"
    fi

    # 데이터 초기화
    curl -s -X POST "$BASE_URL/reserve/init/reset?scheduleId=$SCHEDULE_ID" > /dev/null
    sleep $RESET_WAIT
    echo "  데이터 초기화 완료"
done

# ---- 4. 결과 비교표 ----
echo ""
echo "[4/4] 최종 결과"

L="====================================================================="
D="---------------------------------------------------------------------"

echo ""
echo "$L"
echo "  예약 부하 테스트 — max-capacity 산정 결과"
echo "$L"
printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s\n" \
       "VU" "성공률" "avg(ms)" "p95(ms)" "p99(ms)" "max(ms)"
echo "$D"

RECOMMENDED=""

for VUS in "${VU_LEVELS[@]}"; do
    FILE="$RESULT_DIR/vu_${VUS}.txt"

    if [ ! -f "$FILE" ]; then
        printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s\n" \
               "$VUS" "N/A" "N/A" "N/A" "N/A" "N/A"
        continue
    fi

    SUCCESS=$(grep "^success=" "$FILE" | cut -d= -f2 | tr -d '\r')
    FAIL=$(grep "^fail=" "$FILE" | cut -d= -f2 | tr -d '\r')
    AVG=$(grep "^avg=" "$FILE" | cut -d= -f2 | tr -d '\r')
    P95=$(grep "^p95=" "$FILE" | cut -d= -f2 | tr -d '\r')
    P99=$(grep "^p99=" "$FILE" | cut -d= -f2 | tr -d '\r')
    MAX=$(grep "^max=" "$FILE" | cut -d= -f2 | tr -d '\r')

    TOTAL=$(( ${SUCCESS:-0} + ${FAIL:-0} ))

    if [ "$TOTAL" -gt 0 ]; then
        RATE=$(awk "BEGIN { printf \"%.1f%%\", (${SUCCESS:-0}/${TOTAL})*100 }")
    else
        RATE="0.0%"
    fi

    printf "  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s\n" \
           "$VUS" "$RATE" "${AVG:-N/A}" "${P95:-N/A}" "${P99:-N/A}" "${MAX:-N/A}"

    # p95 기준으로 권장 max-capacity 산정
    if [ -n "$P95" ]; then
        IS_UNDER=$(awk "BEGIN { print (${P95} < ${P95_THRESHOLD}) ? 1 : 0 }")
        SUCCESS_RATE_OK=$(awk "BEGIN { print (${SUCCESS:-0}/${TOTAL} > 0.99) ? 1 : 0 }")
        if [ "$IS_UNDER" = "1" ] && [ "$SUCCESS_RATE_OK" = "1" ]; then
            RECOMMENDED=$VUS
        fi
    fi
done

echo "$D"
echo ""

if [ -n "$RECOMMENDED" ]; then
    echo "  권장 max-capacity: ${RECOMMENDED}"
    echo "  (기준: p95 < ${P95_THRESHOLD}ms 이면서 성공률 > 99%)"
else
    echo "  모든 VU 단계에서 기준 미달 — 인프라 스케일업 또는 VU 범위 조정 필요"
    echo "  (기준: p95 < ${P95_THRESHOLD}ms, 성공률 > 99%)"
fi

echo ""
echo "$L"
echo ""
echo "  결과 파일: $(pwd)/$RESULT_DIR/"
echo "  p95 임계값 변경: P95_THRESHOLD=300 ./reserve-benchmark.sh"
echo ""
