// 대기열 부하 테스트 — 고정 RPS로 등록 요청을 전송
//
// 측정 범위 (HTTP 응답 시간 = E2E):
//   [1] enqueue-or-allow.lua — 중복 체크 + score 생성 + 대기열/참가열 삽입 (원자적)
//   [2] addActiveQueue + Redis Pub/Sub 알림
//
// [ 실행 ]
//   K6_RATE=300 k6 run queue-load-test.js
//   K6_RATE=50 K6_DURATION=1 k6 run queue-load-test.js   (웜업용)
//
// [ 환경 변수 ]
//   K6_RATE     : 초당 요청 수 (기본값 300)
//   K6_DURATION : 테스트 지속 시간 초 (기본값 10)

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

var successCount = new Counter('success_count');
var failCount    = new Counter('fail_count');
var duplicateCount = new Counter('duplicate_count');

var RATE = parseInt(__ENV.K6_RATE || '300', 10);
var DURATION = parseInt(__ENV.K6_DURATION || '10', 10);
var TOTAL_EXPECTED = RATE * DURATION;

export var options = {
    scenarios: {
        load_test: {
            executor: 'constant-arrival-rate',
            rate: RATE,
            timeUnit: '1s',
            duration: DURATION + 's',
            preAllocatedVUs: Math.min(Math.ceil(RATE * 2), 2000),
            maxVUs: Math.min(Math.ceil(RATE * 5), 5000),
            gracefulStop: '30s',
        },
    },
    summaryTrendStats: ['min', 'avg', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
};

export default function () {
    var uid = 'user-' + __VU + '-' + __ITER;
    var payload = JSON.stringify({
        queueType: 'concert',
        userId: uid,
    });
    var params = {
        headers: {
            'Content-Type': 'application/json',
            'X-Request-Timestamp': '' + (Date.now() / 1000),
        },
        timeout: '30s',
    };

    var res = http.post('http://localhost:8079/queue/register', payload, params);
    var body = (res.body || '').replace(/"/g, '').trim();

    if (res.status === 200 && (body === 'QUEUED' || body === 'DIRECT_ALLOW')) {
        successCount.add(1);
    } else if (res.status === 200 && (body === 'ALREADY_IN_WAIT' || body === 'ALREADY_IN_ALLOW')) {
        duplicateCount.add(1);
    } else {
        failCount.add(1);
    }

    check(res, {
        '등록 성공 (200)': function (r) { return r.status === 200; },
    });
}

export function handleSummary(data) {
    var d = data.metrics.http_req_duration.values;
    var success = data.metrics.success_count ? data.metrics.success_count.values.count : 0;
    var fail = data.metrics.fail_count ? data.metrics.fail_count.values.count : 0;
    var dup = data.metrics.duplicate_count ? data.metrics.duplicate_count.values.count : 0;
    var total = success + fail + dup;

    var line = '='.repeat(55);
    var dash = '-'.repeat(55);
    var successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '0.0';

    var output = '\n' + dash + '\n';
    output += '  HTTP 응답 측정 — ' + TOTAL_EXPECTED + '건 / ' + DURATION + '초 (' + RATE + ' RPS)\n';
    output += dash + '\n';
    output += '  성공: ' + success + '건 (' + successRate + '%)  ';
    output += '중복: ' + dup + '건  실패: ' + fail + '건\n';
    output += '  응답 시간  min=' + d.min.toFixed(2) + 'ms  avg=' + d.avg.toFixed(2) + 'ms  max=' + d.max.toFixed(2) + 'ms\n';
    output += '  p(95)=' + d['p(95)'].toFixed(2) + 'ms  p(99)=' + d['p(99)'].toFixed(2) + 'ms\n';
    output += dash + '\n';

    // benchmark_test.sh가 파싱할 결과 파일
    var result = [
        'success=' + success,
        'fail=' + fail,
        'duplicate=' + dup,
        'http_min=' + d.min.toFixed(2),
        'http_avg=' + d.avg.toFixed(2),
        'http_med=' + d.med.toFixed(2),
        'http_max=' + d.max.toFixed(2),
        'http_p95=' + d['p(95)'].toFixed(2),
        'http_p99=' + d['p(99)'].toFixed(2),
    ].join('\n') + '\n';

    return {
        stdout: output,
        'k6_result.txt': result,
    };
}
