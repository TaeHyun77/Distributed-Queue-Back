#!/bin/bash
# 예약 부하 벤치마크 — 예약 시스템 한계 측정 및 max-capacity 산정
#
# VU(동시 사용자)를 단계적으로 올리며 예약 시스템의 처리 한계를 측정합니다.
# 각 단계마다 docker-compose up/down을 반복하여 JVM/Redis를 초기화합니다.
# MySQL 볼륨은 보존하여 사전 생성된 유저/좌석 데이터를 유지합니다.
#
# 한계 판단 기준:
#   - 성공률 100% 미만 (실패 발생)
#   - p99 응답 시간이 이전 단계 대비 2배 이상 급등
#   - 위 기준에 도달하기 직전 단계의 VU를 권장 max-capacity로 제시
#
# 사용법:
#   ./reserve-benchmark-test.sh                        → 기본 (VU: 100 150 200 250 300 350 400 450 500)
#   ./reserve-benchmark-test.sh "100 200 300 400 500"  → 커스텀 VU 단계
#
# 사전 조건:
#   - k6 설치
#   - docker, docker-compose 설치
#   - reserve-load-test.js 동일 디렉터리에 존재
#   - MySQL에 loadtest 유저 500명 + 좌석 5000개가 사전 생성되어 있어야 함

set -euo pipefail

# ---- 설정 ----
VU_LEVELS=(${1:-100 150 200 250 300 350 400 450 500})
BASE_URL="http://localhost:8079"
COMPOSE_FILE="../docker-compose.yml"
HEALTHCHECK_TIMEOUT=120
STABILIZE_WAIT=5
RESULT_DIR="reserve_benchmark_results"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_START=$(date +%s)

echo ""
echo "======================================================="
echo "  예약 부하 벤치마크 — 시스템 한계 측정"
echo "  VU 단계: ${VU_LEVELS[*]}"
echo "  각 단계마다 docker up/down 반복 (MySQL 볼륨 보존)"
echo "  시작 시각: $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================="

# ---- [1/3] 사전 검증 ----
echo ""
echo "[1/3] 사전 검증"

# k6 설치 확인
if ! command -v k6 &> /dev/null; then
    echo "  오류: k6가 설치되지 않았습니다. https://k6.io/docs/get-started/installation/"
    exit 1
fi
echo "  k6: $(k6 version 2>/dev/null | head -1)"

# docker 설치 확인
if ! command -v docker &> /dev/null; then
    echo "  오류: docker가 설치되지 않았습니다."
    exit 1
fi
echo "  docker: $(docker --version 2>/dev/null | head -1)"

# docker-compose 또는 docker compose 확인
COMPOSE_CMD=""
if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    echo "  오류: docker-compose가 설치되지 않았습니다."
    exit 1
fi
echo "  compose: $COMPOSE_CMD"

# reserve-load-test.js 존재 확인
if [ ! -f "$SCRIPT_DIR/reserve-load-test.js" ]; then
    echo "  오류: reserve-load-test.js 파일이 없습니다."
    exit 1
fi
echo "  reserve-load-test.js: 확인"

# docker-compose.yml 존재 확인
if [ ! -f "$SCRIPT_DIR/$COMPOSE_FILE" ]; then
    echo "  오류: $COMPOSE_FILE 파일이 없습니다."
    exit 1
fi
echo "  docker-compose.yml: 확인"

# 결과 디렉터리 생성
mkdir -p "$SCRIPT_DIR/$RESULT_DIR"
echo "  결과 디렉터리: $RESULT_DIR/"

# VU 최대값 검증 (테스트 유저 500명, 좌석 5000개 한도)
MAX_VU=0
for v in "${VU_LEVELS[@]}"; do
    if [ "$v" -gt "$MAX_VU" ]; then MAX_VU=$v; fi
done
if [ "$MAX_VU" -gt 500 ]; then
    echo "  경고: VU 최대값(${MAX_VU})이 테스트 유저 한도(500명)를 초과합니다."
    echo "  500 초과 단계는 로그인 실패가 발생합니다."
fi

echo ""
echo "  VU 단계: ${VU_LEVELS[*]}"
echo "  총 ${#VU_LEVELS[@]}단계 테스트 예정"

# ---- 헬스체크 함수 ----
# HTTP 응답 코드 기반으로 서비스 기동 확인
# 401(인증 필요)도 서비스가 기동된 것으로 판단 (connection refused와 구분)
wait_for_service() {
    local url="$1"
    local timeout="$2"
    local start=$(date +%s)

    echo "    서비스 기동 대기 중... (최대 ${timeout}초)" >&2
    while true; do
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null) || http_code="000"

        if [ "$http_code" != "000" ]; then
            echo "    서비스 준비 완료 (HTTP ${http_code})" >&2
            return 0
        fi

        local now=$(date +%s)
        local elapsed=$((now - start))
        if [ $elapsed -ge $timeout ]; then
            echo "    오류: 서비스 기동 타임아웃 (${timeout}초)" >&2
            return 1
        fi

        sleep 3
    done
}

# ---- 인증 헬퍼 함수 ----
# loadtest1 유저로 로그인하여 JWT 토큰 추출
get_auth_token() {
    local base_url="$1"
    local response_headers
    response_headers=$(curl -s -D - -o /dev/null \
        -X POST "${base_url}/reserve/login" \
        -d "username=loadtest1&password=test1234" \
        -H "Content-Type: application/x-www-form-urlencoded" 2>/dev/null)

    local token
    token=$(echo "$response_headers" | grep -i "^Access:" | sed 's/^[Aa]ccess: *//;s/\r$//')

    if [ -z "$token" ]; then
        echo "" # 빈 문자열 반환
        return 1
    fi

    echo "$token"
    return 0
}

# ---- docker-compose 헬퍼 ----
compose_up() {
    echo "    docker-compose up -d..."
    (cd "$SCRIPT_DIR/.." && $COMPOSE_CMD -f docker-compose.yml up -d) > /dev/null 2>&1
}

compose_down() {
    echo "    docker-compose down (볼륨 보존)..."
    (cd "$SCRIPT_DIR/.." && $COMPOSE_CMD -f docker-compose.yml down) > /dev/null 2>&1
}

# ---- [2/3] VU별 테스트 ----
echo ""
echo "[2/3] VU별 테스트 시작..."

# 결과 저장용 배열
declare -a ALL_VU ALL_SUCCESS ALL_FAIL ALL_MIN ALL_AVG ALL_MED ALL_P90 ALL_P95 ALL_P99 ALL_MAX
IDX=0

for VU in "${VU_LEVELS[@]}"; do
    echo ""
    echo "  ══════════════════════════════════════"
    echo "  VU=${VU} 테스트 시작"
    echo "  ══════════════════════════════════════"

    # a. docker-compose up
    compose_up

    # b. 헬스체크 (HTTP 응답 대기, 401도 OK)
    if ! wait_for_service "${BASE_URL}/reserve/init/load-test" $HEALTHCHECK_TIMEOUT; then
        echo "    서비스 기동 실패, 이 단계를 건너뜁니다."
        compose_down
        ALL_VU[$IDX]=$VU; ALL_SUCCESS[$IDX]=0; ALL_FAIL[$IDX]=0
        ALL_MIN[$IDX]=0; ALL_AVG[$IDX]=0; ALL_MED[$IDX]=0
        ALL_P90[$IDX]=0; ALL_P95[$IDX]=0; ALL_P99[$IDX]=0; ALL_MAX[$IDX]=0
        IDX=$((IDX + 1))
        continue
    fi

    # c. 로그인 → 토큰 확보
    echo "    로그인 중 (loadtest1)..."
    AUTH_TOKEN=""
    AUTH_TOKEN=$(get_auth_token "$BASE_URL") || true

    if [ -z "$AUTH_TOKEN" ]; then
        echo "    오류: 로그인 실패 (토큰 취득 불가)"
        compose_down
        ALL_VU[$IDX]=$VU; ALL_SUCCESS[$IDX]=0; ALL_FAIL[$IDX]=0
        ALL_MIN[$IDX]=0; ALL_AVG[$IDX]=0; ALL_MED[$IDX]=0
        ALL_P90[$IDX]=0; ALL_P95[$IDX]=0; ALL_P99[$IDX]=0; ALL_MAX[$IDX]=0
        IDX=$((IDX + 1))
        continue
    fi
    echo "    토큰 취득 완료"

    # d. POST /reserve/init/load-test (인증) → scheduleId 파싱
    echo "    데이터 초기화 중..."
    INIT_RESPONSE=$(curl -sf -X POST "${BASE_URL}/reserve/init/load-test" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" 2>/dev/null) || INIT_RESPONSE=""

    if [ -z "$INIT_RESPONSE" ]; then
        echo "    오류: init/load-test API 호출 실패"
        compose_down
        ALL_VU[$IDX]=$VU; ALL_SUCCESS[$IDX]=0; ALL_FAIL[$IDX]=0
        ALL_MIN[$IDX]=0; ALL_AVG[$IDX]=0; ALL_MED[$IDX]=0
        ALL_P90[$IDX]=0; ALL_P95[$IDX]=0; ALL_P99[$IDX]=0; ALL_MAX[$IDX]=0
        IDX=$((IDX + 1))
        continue
    fi

    # scheduleId 추출 (JSON 응답에서 숫자 파싱)
    SCHEDULE_ID=$(echo "$INIT_RESPONSE" | grep -oE '"scheduleId"\s*:\s*[0-9]+' | grep -oE '[0-9]+' || \
                  echo "$INIT_RESPONSE" | grep -oE '[0-9]+' | head -1)

    if [ -z "$SCHEDULE_ID" ]; then
        echo "    오류: scheduleId 파싱 실패 (응답: ${INIT_RESPONSE:0:200})"
        compose_down
        ALL_VU[$IDX]=$VU; ALL_SUCCESS[$IDX]=0; ALL_FAIL[$IDX]=0
        ALL_MIN[$IDX]=0; ALL_AVG[$IDX]=0; ALL_MED[$IDX]=0
        ALL_P90[$IDX]=0; ALL_P95[$IDX]=0; ALL_P99[$IDX]=0; ALL_MAX[$IDX]=0
        IDX=$((IDX + 1))
        continue
    fi

    echo "    scheduleId: $SCHEDULE_ID"

    # e. 웜업: k6 run VUS=10
    echo "    웜업 실행 중 (VUS=10)..."
    (cd "$SCRIPT_DIR" && k6 run -e VUS=10 -e SCHEDULE_ID=$SCHEDULE_ID -e BASE_URL=$BASE_URL reserve-load-test.js) > /dev/null 2>&1 || true
    rm -f "$SCRIPT_DIR/reserve_result.txt"

    # f. 웜업 데이터 초기화 (인증)
    echo "    웜업 데이터 초기화..."
    curl -sf -X POST "${BASE_URL}/reserve/init/reset?scheduleId=${SCHEDULE_ID}" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" > /dev/null 2>&1 || true

    # g. 안정화 대기
    echo "    안정화 대기 (${STABILIZE_WAIT}초)..."
    sleep $STABILIZE_WAIT

    # h. 본 테스트
    echo "    본 테스트 실행 (VUS=${VU})..."
    (cd "$SCRIPT_DIR" && k6 run -e VUS=$VU -e SCHEDULE_ID=$SCHEDULE_ID -e BASE_URL=$BASE_URL reserve-load-test.js) || true

    # i. 결과 파싱
    RESULT_FILE="$SCRIPT_DIR/reserve_result.txt"
    if [ -f "$RESULT_FILE" ]; then
        SUCCESS=$(grep "^success=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        FAIL=$(grep "^fail=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        MIN=$(grep "^min=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        AVG=$(grep "^avg=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        MED=$(grep "^med=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        P90=$(grep "^p90=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        P95=$(grep "^p95=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        P99=$(grep "^p99=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")
        MAX=$(grep "^max=" "$RESULT_FILE" | cut -d= -f2 | tr -d '\r' || echo "0")

        ALL_VU[$IDX]=$VU
        ALL_SUCCESS[$IDX]=${SUCCESS:-0}
        ALL_FAIL[$IDX]=${FAIL:-0}
        ALL_MIN[$IDX]=${MIN:-0}
        ALL_AVG[$IDX]=${AVG:-0}
        ALL_MED[$IDX]=${MED:-0}
        ALL_P90[$IDX]=${P90:-0}
        ALL_P95[$IDX]=${P95:-0}
        ALL_P99[$IDX]=${P99:-0}
        ALL_MAX[$IDX]=${MAX:-0}

        # 결과 파일 보관
        mkdir -p "$SCRIPT_DIR/$RESULT_DIR"
        cp "$RESULT_FILE" "$SCRIPT_DIR/$RESULT_DIR/vu_${VU}.txt"

        TOTAL=$((${SUCCESS:-0} + ${FAIL:-0}))
        if [ "$TOTAL" -gt 0 ] 2>/dev/null; then
            SR=$(awk "BEGIN { printf \"%.1f\", (${SUCCESS:-0} / $TOTAL) * 100 }")
        else
            SR="0.0"
        fi

        echo "    결과: 성공=${SUCCESS:-0} 실패=${FAIL:-0} 성공률=${SR}% avg=${AVG:-0}ms p99=${P99:-0}ms"

        rm -f "$RESULT_FILE"
    else
        echo "    경고: reserve_result.txt 없음"
        ALL_VU[$IDX]=$VU; ALL_SUCCESS[$IDX]=0; ALL_FAIL[$IDX]=0
        ALL_MIN[$IDX]=0; ALL_AVG[$IDX]=0; ALL_MED[$IDX]=0
        ALL_P90[$IDX]=0; ALL_P95[$IDX]=0; ALL_P99[$IDX]=0; ALL_MAX[$IDX]=0
    fi

    IDX=$((IDX + 1))

    # j. 테스트 데이터 초기화 (인증)
    echo "    테스트 데이터 초기화..."
    curl -sf -X POST "${BASE_URL}/reserve/init/reset?scheduleId=${SCHEDULE_ID}" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" > /dev/null 2>&1 || true

    # k. docker-compose down (볼륨 보존)
    compose_down

    echo "  VU=${VU} 완료"
done

# ---- [3/3] 결과 비교표 ----
BENCH_END=$(date +%s)
BENCH_ELAPSED=$((BENCH_END - BENCH_START))
BENCH_MIN=$((BENCH_ELAPSED / 60))
BENCH_SEC=$((BENCH_ELAPSED % 60))

echo ""
echo "[3/3] 최종 결과"

L="============================================================================================================="
D="-------------------------------------------------------------------------------------------------------------"

echo ""
echo "$L"
echo "  예약 부하 벤치마크 — 시스템 한계 측정 결과"
echo "  측정 시각: $(date '+%Y-%m-%d %H:%M:%S')  |  총 소요: ${BENCH_MIN}분 ${BENCH_SEC}초"
echo "$L"
printf "  %-6s  %-6s  %-6s  %-8s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
       "VUS" "성공" "실패" "성공률" "min(ms)" "avg(ms)" "med(ms)" "p90(ms)" "p95(ms)" "p99(ms)" "max(ms)"
echo "$D"

FIRST_FAIL_VU=""
P99_SPIKE_VU=""
PREV_P99=""
PREV_AVG=""
RECOMMENDED=""
PREV_SAFE_VU=""

for ((i=0; i<IDX; i++)); do
    VU=${ALL_VU[$i]}
    SUCCESS=${ALL_SUCCESS[$i]}
    FAIL=${ALL_FAIL[$i]}
    MIN=${ALL_MIN[$i]}
    AVG=${ALL_AVG[$i]}
    MED=${ALL_MED[$i]}
    P90=${ALL_P90[$i]}
    P95=${ALL_P95[$i]}
    P99=${ALL_P99[$i]}
    MAX=${ALL_MAX[$i]}

    TOTAL=$((SUCCESS + FAIL))
    if [ "$TOTAL" -gt 0 ] 2>/dev/null; then
        SR=$(awk "BEGIN { printf \"%.1f\", ($SUCCESS / $TOTAL) * 100 }")
    else
        SR="0.0"
    fi

    # 한계 판단: 성공률 100% 미만
    MARKER=""
    if [ -z "$FIRST_FAIL_VU" ] && [ "$FAIL" -gt 0 ] 2>/dev/null; then
        FIRST_FAIL_VU=$VU
        MARKER=" ← 실패 발생"
    fi

    # 한계 판단: p99 급등 (이전 대비 2배 이상)
    if [ -n "$PREV_P99" ] && [ -z "$P99_SPIKE_VU" ]; then
        IS_SPIKE=$(awk "BEGIN { print ($P99 >= $PREV_P99 * 2) ? 1 : 0 }" 2>/dev/null || echo "0")
        if [ "$IS_SPIKE" = "1" ]; then
            P99_SPIKE_VU=$VU
            if [ -z "$MARKER" ]; then
                MARKER=" ← p99 급등"
            else
                MARKER="$MARKER + p99 급등"
            fi
        fi
    fi

    # avg 변화율 계산
    AVG_CHANGE=""
    if [ -n "$PREV_AVG" ]; then
        AVG_CHANGE=$(awk "BEGIN { if ($PREV_AVG > 0) printf \"(+%.0f%%)\", (($AVG - $PREV_AVG) / $PREV_AVG) * 100; else print \"\" }" 2>/dev/null || echo "")
    fi

    # 안전한 마지막 VU 추적
    if [ "$FAIL" -eq 0 ] 2>/dev/null && [ -z "$P99_SPIKE_VU" ]; then
        PREV_SAFE_VU=$VU
    fi

    # avg 열에 변화율 병합
    AVG_DISPLAY="${AVG}"
    if [ -n "$AVG_CHANGE" ]; then
        AVG_DISPLAY="${AVG} ${AVG_CHANGE}"
    fi

    printf "  %-6s  %-6s  %-6s  %-8s  %-10s  %-18s  %-10s  %-10s  %-10s  %-10s  %-10s%s\n" \
           "$VU" "$SUCCESS" "$FAIL" "${SR}%" "$MIN" "$AVG_DISPLAY" "$MED" "$P90" "$P95" "$P99" "$MAX" "$MARKER"

    PREV_P99=$P99
    PREV_AVG=$AVG
done

echo "$D"
echo ""

# 권장 max-capacity 산정
if [ -n "$FIRST_FAIL_VU" ] || [ -n "$P99_SPIKE_VU" ]; then
    # 한계 지점이 발견된 경우 — 그 직전 VU를 권장
    if [ -n "$PREV_SAFE_VU" ]; then
        RECOMMENDED=$PREV_SAFE_VU
    fi

    if [ -n "$FIRST_FAIL_VU" ]; then
        echo "  첫 실패 발생 지점: VU=${FIRST_FAIL_VU}"
    fi
    if [ -n "$P99_SPIKE_VU" ]; then
        echo "  p99 급등 지점: VU=${P99_SPIKE_VU} (이전 대비 2배 이상)"
    fi
    echo ""

    if [ -n "$RECOMMENDED" ]; then
        echo "  ★ 권장 max-capacity: ${RECOMMENDED}"
        echo "    (한계 직전 단계의 VU, 성공률 100% + p99 안정)"
    else
        echo "  ★ 첫 단계부터 한계 도달 — 더 낮은 VU로 재테스트 필요"
    fi
else
    echo "  모든 단계에서 성공률 100% + p99 안정"
    echo "  ★ 테스트 범위 내 한계 미도달 — 더 높은 VU로 재테스트 필요"
    echo "    (테스트된 최대 VU: ${ALL_VU[$((IDX-1))]})"
fi

# CSV 결과 저장
CSV_FILE="$SCRIPT_DIR/$RESULT_DIR/benchmark_$(date '+%Y%m%d_%H%M%S').csv"
echo "VUS,성공,실패,성공률(%),min(ms),avg(ms),med(ms),p90(ms),p95(ms),p99(ms),max(ms)" > "$CSV_FILE"
for ((i=0; i<IDX; i++)); do
    TOTAL=$((${ALL_SUCCESS[$i]} + ${ALL_FAIL[$i]}))
    if [ "$TOTAL" -gt 0 ] 2>/dev/null; then
        SR=$(awk "BEGIN { printf \"%.1f\", (${ALL_SUCCESS[$i]} / $TOTAL) * 100 }")
    else
        SR="0.0"
    fi
    echo "${ALL_VU[$i]},${ALL_SUCCESS[$i]},${ALL_FAIL[$i]},${SR},${ALL_MIN[$i]},${ALL_AVG[$i]},${ALL_MED[$i]},${ALL_P90[$i]},${ALL_P95[$i]},${ALL_P99[$i]},${ALL_MAX[$i]}" >> "$CSV_FILE"
done

echo ""
echo "$L"
echo ""
echo "  결과 파일: $(cd "$SCRIPT_DIR" && pwd)/$RESULT_DIR/"
echo "  CSV 파일: $CSV_FILE"
echo ""
