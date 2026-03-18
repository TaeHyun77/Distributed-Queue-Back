import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

var successCount = new Counter('success_count');
var failCount = new Counter('fail_count');

// ========================
// 동시성 제어 검증 테스트
// 여러 VU가 동일 좌석을 동시에 예약 시도
// 기대 결과 : 1명만 성공, 나머지는 정상 실패

// [ 테스트 방법 ] : k6 run -e SCHEDULE_ID=10 -e TARGET_VUS=10 contention-test.js
// - initLoadTestData가 존재하는지
// - 테스트 한 후 resetLoadTestData 호출
// ========================

const BASE_URL = 'http://localhost:8079';
const SCHEDULE_ID = __ENV.SCHEDULE_ID || '10';
const VUS = parseInt(__ENV.TARGET_VUS || '10');

export const options = {
    vus: VUS,
    iterations: VUS,
};

const users = new SharedArray('users', function () {
    const arr = [];
    for (let i = 1; i <= 500; i++) {
        arr.push({ username: `loadtest${i}`, password: 'test1234' });
    }
    return arr;
});

export default function () {
    const vuId = __VU;
    const user = users[(vuId - 1) % users.length];

    // 로그인
    const loginRes = http.post(
        `${BASE_URL}/reserve/login`,
        `username=${user.username}&password=${user.password}`,
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    var token = '';
    var hdrs = loginRes.headers;
    if (hdrs['Access']) { token = hdrs['Access']; }
    else if (hdrs['access']) { token = hdrs['access']; }
    else {
        // 헤더 키 전체를 순회하며 찾기
        for (var key in hdrs) {
            if (key.toLowerCase() === 'access') {
                token = hdrs[key];
                break;
            }
        }
    }

    if (loginRes.status !== 200) {
        console.log('VU ' + vuId + ' 로그인 실패: status=' + loginRes.status + ' body=' + loginRes.body);
    }
    if (!token) {
        console.log('VU ' + vuId + ' 토큰 없음. 응답 헤더 키: ' + Object.keys(hdrs).join(', '));
    }

    // 모든 VU가 동일 좌석(T1)을 예약 시도
    const res = http.post(
        BASE_URL + '/reserve',
        JSON.stringify({
            performanceScheduleId: parseInt(SCHEDULE_ID),
            reservedSeat: ['T1'],
        }),
        {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + token,
                'Idempotency-Key': uuidv4(),
            },
        }
    );

    var success = res.status === 200 || res.status === 201;

    if (success) {
        successCount.add(1);
    } else {
        failCount.add(1);
    }

    check(res, {
        '성공 또는 정상 실패': function (r) {
            return r.status === 200 || r.status === 201 || r.status === 409 || r.status === 400;
        },
    });

    console.log('VU ' + vuId + ' (' + user.username + '): status=' + res.status + ' ' + (success ? '예약 성공' : '예약 실패 (정상)') + ' body=' + res.body);
}

export function handleSummary(data) {
    var total = data.metrics.http_reqs && data.metrics.http_reqs.values && data.metrics.http_reqs.values.count || 0;
    var s200 = data.metrics.http_req_duration && data.metrics.http_req_duration.values ? 0 : 0;

    var successCount = data.metrics['success_count'] && data.metrics['success_count'].values && data.metrics['success_count'].values.count || 0;
    var failCount = data.metrics['fail_count'] && data.metrics['fail_count'].values && data.metrics['fail_count'].values.count || 0;

    console.log('\n========== 동시성 제어 검증 결과 ==========');
    console.log('총 예약 요청: ' + (successCount + failCount));
    console.log('200 성공: ' + successCount + '건');
    console.log('실패 (409/400 등): ' + failCount + '건');

    if (successCount === 1) {
        console.log('>> 1명만 예약 완료');
    } else if (successCount === 0) {
        console.log('>> 성공 요청 0건');
    } else {
        console.log('>> 중복 예약 발생! ' + successCount + '명이 동시에 성공 - 동시성 제어 실패');
    }

    console.log('==========================================\n');
    return {};
}