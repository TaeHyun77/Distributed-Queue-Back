import http from 'k6/http';
import { check } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { Counter } from 'k6/metrics';

const successCount = new Counter('success_count');
const failCount    = new Counter('fail_count');

export const options = {
    scenarios: {
        // 본 테스트: 10초 × 300 RPS = 3,000건
        load_test: {
            executor: 'constant-arrival-rate',
            rate: 500,
            timeUnit: '1s',
            duration: '10s',
            preAllocatedVUs: 500,
            maxVUs: 1000,
        },
    },

    thresholds: {
        'http_req_duration': ['p(95)<1000', 'p(99)<2000'],
        'checks':           ['rate>0.95'],
        'fail_count':       ['count<100'],
    },

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
            'X-Request-Timestamp': `${Date.now() / 1000}`,
        },
        timeout: '10s',
    };

    const res = http.post('http://localhost:8079/queue/register', payload, params);

    const passed = check(res, {
        'status is 200': (r) => r.status === 200,
    });

    if (passed) {
        successCount.add(1);
    } else {
        failCount.add(1);
        if (__ITER < 50) {
            console.warn(
                `FAIL VU:${__VU} iter:${__ITER} status:${res.status} ` +
                `body:${(res.body || '').substring(0, 200)}`
            );
        }
    }
}