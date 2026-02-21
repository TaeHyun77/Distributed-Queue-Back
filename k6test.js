import http from 'k6/http';
import { check } from 'k6';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

export const options = {
    scenarios: {
        ordered: {
            executor: 'constant-arrival-rate',
            rate: 10,             // 초당 10건
            timeUnit: '1s',
            duration: '10s',      // 10초 동안 총 100건
            preAllocatedVUs: 20,
        },
    },
};

export default function () {
    const payload = JSON.stringify({
        queueType: 'concert',
        userId: `user-${__VU}`,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'request-key': `${__VU}-${randomString(8)}`,
        },
    };

    const res = http.post('http://localhost:8079/queue/register', payload, params);

    check(res, {
        'status is 200': (r) => r.status === 200,
    });

    if (res.status !== 200) {
        console.log(`VU:${__VU} status:${res.status} body:${res.body}`);
    }
}