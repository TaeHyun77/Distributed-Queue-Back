import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// ========================
// 예약 부하 테스트 — 대기열 max-capacity 산정
//
// N명이 동시에 예약 요청을 보내는 버스트 테스트를 수행하여
// 시스템이 안정적으로 처리할 수 있는 최대 동시 사용자 수를 측정합니다.
//
// 측정 항목:
//   - reserve_duration: 순수 예약 API 응답 시간 (setup 로그인 제외)
//   - reserve_success / reserve_fail: 성공/실패 건수
//
// 테스트 설계:
//   - 각 VU는 고유한 유저(loadtest{N})로 로그인
//   - 각 VU는 고유한 좌석(T{N})을 예약 → 좌석 경합 없이 순수 처리량 측정
//   - per-vu-iterations 실행기로 모든 VU가 동시에 1건씩 예약 (버스트)
//
// [ 실행 방법 ]
//   k6 run -e SCHEDULE_ID=10 -e VUS=100 reserve-load-test.js
//
// [ 환경 변수 ]
//   VUS          : 동시 사용자 수 (기본값 50, 최대 500)
//   SCHEDULE_ID  : 테스트용 공연 일정 ID
//   BASE_URL     : 서버 URL (기본값 http://localhost:8079)
//
// [ 사전 조건 ]
//   1. docker-compose up (reserve 시스템 기동)
//   2. POST /reserve/init/load-test 호출 → scheduleId 확인
//   3. 테스트 후 POST /reserve/init/reset?scheduleId=X 으로 초기화
// ========================

var BASE_URL = __ENV.BASE_URL || 'http://localhost:8079';
var SCHEDULE_ID = parseInt(__ENV.SCHEDULE_ID || '10');
var VUS = parseInt(__ENV.VUS || '50');

// 커스텀 메트릭 — setup()의 로그인 요청을 제외하고 순수 예약 성능만 측정
var reserveSuccess = new Counter('reserve_success');
var reserveFail = new Counter('reserve_fail');
var reserveDuration = new Trend('reserve_duration', true);

// 테스트 유저 500명 (SharedArray는 VU 간 메모리 공유)
var users = new SharedArray('users', function () {
    var arr = [];
    for (var i = 1; i <= 500; i++) {
        arr.push({ username: 'loadtest' + i, password: 'test1234' });
    }
    return arr;
});

export var options = {
    scenarios: {
        burst: {
            executor: 'per-vu-iterations',
            vus: VUS,
            iterations: 1,
        },
    },
    summaryTrendStats: ['min', 'avg', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
};

function extractToken(res) {
    var hdrs = res.headers;
    for (var key in hdrs) {
        if (key.toLowerCase() === 'access') {
            return hdrs[key];
        }
    }
    return '';
}

// setup: 모든 VU의 토큰을 미리 발급 (측정 대상에서 제외)
export function setup() {
    var actualVUs = Math.min(VUS, 500);

    if (VUS > 500) {
        console.log('경고: VUS(' + VUS + ')가 최대 유저 수(500)를 초과합니다. 500으로 제한됩니다.');
    }

    console.log(actualVUs + '명 사용자 로그인 중...');

    var tokens = [];
    var batchSize = 50;
    var loginFailCount = 0;

    for (var start = 0; start < actualVUs; start += batchSize) {
        var end = Math.min(start + batchSize, actualVUs);
        var batch = [];

        for (var i = start; i < end; i++) {
            batch.push([
                'POST',
                BASE_URL + '/reserve/login',
                'username=' + users[i].username + '&password=' + users[i].password,
                { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
            ]);
        }

        var responses = http.batch(batch);

        for (var j = 0; j < responses.length; j++) {
            var token = extractToken(responses[j]);
            if (!token) {
                loginFailCount++;
                if (loginFailCount <= 3) {
                    console.log('  로그인 실패: ' + users[start + j].username
                        + ' (status=' + responses[j].status + ')');
                }
            }
            tokens.push(token);
        }
    }

    var loginSuccessCount = actualVUs - loginFailCount;
    console.log('로그인 완료: ' + loginSuccessCount + '/' + actualVUs + '명 성공');

    if (loginFailCount > 0) {
        console.log('로그인 실패: ' + loginFailCount + '명 (해당 VU는 건너뜁니다)');
    }

    return { tokens: tokens };
}

// 각 VU: 고유 유저로 고유 좌석 1건 예약
export default function (data) {
    var vuIndex = __VU - 1;
    var token = data.tokens[vuIndex];
    var seatNumber = 'T' + __VU;

    if (!token) {
        reserveFail.add(1);
        return;
    }

    var res = http.post(
        BASE_URL + '/reserve',
        JSON.stringify({
            performanceScheduleId: SCHEDULE_ID,
            reservedSeat: [seatNumber],
        }),
        {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + token,
                'Idempotency-Key': uuidv4(),
            },
        }
    );

    // 커스텀 메트릭에 순수 예약 응답 시간만 기록
    reserveDuration.add(res.timings.duration);

    var success = res.status === 200 || res.status === 201;

    if (success) {
        reserveSuccess.add(1);
    } else {
        reserveFail.add(1);
        console.log('VU ' + __VU + ' 예약 실패: status=' + res.status
            + ' body=' + (res.body || '').substring(0, 200));
    }

    check(res, {
        '예약 성공 (200/201)': function (r) { return r.status === 200 || r.status === 201; },
    });
}

export function handleSummary(data) {
    var rd = data.metrics.reserve_duration ? data.metrics.reserve_duration.values : null;
    var success = data.metrics.reserve_success
        ? data.metrics.reserve_success.values.count : 0;
    var fail = data.metrics.reserve_fail
        ? data.metrics.reserve_fail.values.count : 0;
    var total = success + fail;
    var successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '0.0';

    var line = '='.repeat(55);
    var dash = '-'.repeat(55);

    var output = '\n' + line + '\n';
    output += '  예약 부하 테스트 결과 — 동시 ' + VUS + '명 (버스트)\n';
    output += line + '\n';
    output += '  성공: ' + success + '건 (' + successRate + '%)  ';
    output += '실패: ' + fail + '건\n';
    output += dash + '\n';

    if (rd) {
        output += '  예약 응답 시간 (reserve_duration):\n';
        output += '    최소   : ' + rd.min.toFixed(2) + 'ms\n';
        output += '    평균   : ' + rd.avg.toFixed(2) + 'ms\n';
        output += '    중앙값 : ' + rd.med.toFixed(2) + 'ms\n';
        output += '    p(90)  : ' + rd['p(90)'].toFixed(2) + 'ms\n';
        output += '    p(95)  : ' + rd['p(95)'].toFixed(2) + 'ms\n';
        output += '    p(99)  : ' + rd['p(99)'].toFixed(2) + 'ms\n';
        output += '    최대   : ' + rd.max.toFixed(2) + 'ms\n';
    } else {
        output += '  측정된 예약 응답 없음\n';
    }

    output += line + '\n';

    // reserve-benchmark.sh 가 파싱할 결과 파일
    var result = '';
    result += 'vus=' + VUS + '\n';
    result += 'success=' + success + '\n';
    result += 'fail=' + fail + '\n';

    if (rd) {
        result += 'min=' + rd.min.toFixed(2) + '\n';
        result += 'avg=' + rd.avg.toFixed(2) + '\n';
        result += 'med=' + rd.med.toFixed(2) + '\n';
        result += 'p90=' + rd['p(90)'].toFixed(2) + '\n';
        result += 'p95=' + rd['p(95)'].toFixed(2) + '\n';
        result += 'p99=' + rd['p(99)'].toFixed(2) + '\n';
        result += 'max=' + rd.max.toFixed(2) + '\n';
    }

    return {
        stdout: output,
        'reserve_result.txt': result,
    };
}
