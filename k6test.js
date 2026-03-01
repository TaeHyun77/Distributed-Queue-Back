import http from 'k6/http';
import { check } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { Counter } from 'k6/metrics';

const successCount = new Counter('success_count');
const failCount = new Counter('fail_count');

export const options = {
    scenarios: {
        warmup: {
            executor: 'constant-arrival-rate',
            rate: 40,              // 초당 40건
            timeUnit: '1s',
            duration: '5s',        // 5초 동안 → 총 200건
            preAllocatedVUs: 100,
            maxVUs: 300,
            tags: { phase: 'warmup' },
        },
        load_test: {
            executor: 'constant-arrival-rate',
            rate: 300,             // 초당 300건
            timeUnit: '1s',
            duration: '10s',       // 10초 동안 → 총 3000건
            preAllocatedVUs: 500,
            maxVUs: 1000,
            startTime: '7s',       // 워밍업(5s) + 대기(2s) 후 시작
            tags: { phase: 'test' },
        },
    },
    thresholds: {
        'http_req_duration{phase:test}': ['p(95)<1000'],
        'checks{phase:test}': ['rate>0.95'],
    },
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
    };

    const res = http.post('http://localhost:8079/queue/register', payload, params);

    const passed = check(res, {
        'status is 200': (r) => r.status === 200,
    });

    if (passed) {
        successCount.add(1);
    } else {
        failCount.add(1);
        console.log(`VU:${__VU} status:${res.status} body:${res.body}`);
    }
}
