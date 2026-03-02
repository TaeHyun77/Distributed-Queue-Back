import http from 'k6/http';
import { check } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { Counter, Trend } from 'k6/metrics';

// ── 커스텀 메트릭 ──
const successCount = new Counter('success_count');
const failCount    = new Counter('fail_count');

// phase별 성공/실패 분리 추적
const warmupSuccess = new Counter('warmup_success');
const loadSuccess   = new Counter('load_success');
const loadFail      = new Counter('load_fail');

export const options = {
    scenarios: {
        // Warmup: JVM/커넥션 풀 예열, 5초 × 40 RPS = 200건
        warmup: {
            executor: 'constant-arrival-rate',
            rate: 40,
            timeUnit: '1s',
            duration: '5s',
            preAllocatedVUs: 100,
            maxVUs: 200,
            tags: { phase: 'warmup' },
        },

        // ── 본 테스트: 10초 × 300 RPS = 3,000건 ──
        load_test: {
            executor: 'constant-arrival-rate',
            rate: 300,
            timeUnit: '1s',
            duration: '10s',
            preAllocatedVUs: 500,
            maxVUs: 1000,
            startTime: '7s',  // warmup( 5초 ) + gap( 2초 )
            tags: { phase: 'test' },
        },
    },

    thresholds: {
        // 본 테스트 기준으로만 평가
        'http_req_duration{phase:test}': ['p(95)<1000', 'p(99)<2000'],
        'checks{phase:test}':           ['rate>0.95'],

        // 전체( warmup 포함 ) 실패율 모니터링
        'fail_count': ['count<100'],
    },

    // k6 summary에 커스텀 메트릭 포함
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
};

export default function () {
    const payload = JSON.stringify({
        queueType: 'concert',
        userId: `user-${__VU}-${__ITER}`,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'request-key': `${__VU}-${__ITER}-${randomString(8)}`,
        },
        // 개별 요청 타임아웃 (consume이 아닌 HTTP 응답 기준)
        timeout: '10s',
    };

    const res = http.post('http://localhost:8079/queue/register', payload, params);

    const passed = check(res, {
        'status is 200': (r) => r.status === 200,
    });

    // 시나리오 태그로 phase 구분
    const phase = __ENV.K6_SCENARIO || '';

    if (passed) {
        successCount.add(1);
        if (phase === 'warmup') warmupSuccess.add(1);
        else loadSuccess.add(1);
    } else {
        failCount.add(1);
        if (phase !== 'warmup') loadFail.add(1);

        // 실패 시 디버깅 정보 (과도한 로깅 방지: 처음 50건만)
        if (__ITER < 50) {
            console.warn(
                `FAIL VU:${__VU} iter:${__ITER} status:${res.status} ` +
                `body:${(res.body || '').substring(0, 200)}`
            );
        }
    }
}
