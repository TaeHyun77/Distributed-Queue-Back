import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

var successCount = new Counter('success_count');
var replayedCount = new Counter('replayed_count');
var unexpectedCount = new Counter('unexpected_count');

// ========================
// 멱등성 검증 테스트
// 동일 유저가 같은 Idempotency-Key로 동일 요청을 여러 번 전송
// 기대 결과 : 예약은 1건만 생성, 나머지는 캐시된 응답
//
// [ 실행 방법 ]
//   k6 run -e SCHEDULE_ID=10 -e REPEAT=10 ./reserve-idempotency-test.js
//
// [ 사전 조건 ]
//   - POST /reserve/init/load-test 로 테스트 데이터 생성
//   - 테스트 후 POST /reserve/init/reset?scheduleId=10 으로 초기화
// ========================

var BASE_URL = 'http://localhost:8079';
var SCHEDULE_ID = __ENV.SCHEDULE_ID || '10';
var REPEAT = parseInt(__ENV.REPEAT || '10');

export var options = {
    vus: 1,
    iterations: REPEAT,
};

var FIXED_IDEMPOTENCY_KEY = uuidv4();

export function setup() {
    var loginRes = http.post(
        BASE_URL + '/reserve/login',
        'username=loadtest1&password=test1234',
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    var accessToken = '';
    var hdrs = loginRes.headers;
    for (var key in hdrs) {
        if (key.toLowerCase() === 'access') {
            accessToken = hdrs[key];
            break;
        }
    }

    if (loginRes.status !== 200 || !accessToken) {
        console.log('로그인 실패: status=' + loginRes.status);
    }

    return { token: accessToken, idempotencyKey: FIXED_IDEMPOTENCY_KEY };
}

export default function (data) {
    var iteration = __ITER + 1;

    var res = http.post(
        BASE_URL + '/reserve',
        JSON.stringify({
            performanceScheduleId: parseInt(SCHEDULE_ID),
            reservedSeat: ['T1'],
        }),
        {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + data.token,
                'Idempotency-Key': data.idempotencyKey,
            },
        }
    );

    var success = res.status === 200 || res.status === 201;
    var body = res.body || '';

    // 첫 번째 iteration: 최초 응답 본문을 저장
    if (iteration === 1) {
        data.firstBody = body;
    }

    if (success && iteration === 1) {
        successCount.add(1);
        console.log('[' + iteration + '/' + REPEAT + '] 최초 요청 → 예약 성공');
    } else if (success && iteration > 1) {
        // 캐시 응답 여부: 응답 본문이 최초 응답과 동일하면 캐시된 것
        var isCached = body === data.firstBody;
        replayedCount.add(1);
        console.log('[' + iteration + '/' + REPEAT + '] 중복 요청 → 200 응답 (캐시 응답=' + isCached + ')');
    } else {
        unexpectedCount.add(1);
        console.log('[' + iteration + '/' + REPEAT + '] 예상치 못한 응답: status=' + res.status + ' body=' + body);
    }

    check(res, {
        '응답 성공': function (r) { return r.status === 200 || r.status === 201; },
    });
}

export function handleSummary(data) {
    var success = data.metrics['success_count'] && data.metrics['success_count'].values && data.metrics['success_count'].values.count || 0;
    var replayed = data.metrics['replayed_count'] && data.metrics['replayed_count'].values && data.metrics['replayed_count'].values.count || 0;
    var unexpected = data.metrics['unexpected_count'] && data.metrics['unexpected_count'].values && data.metrics['unexpected_count'].values.count || 0;
    var total = success + replayed + unexpected;

    console.log('\n========== 멱등성 검증 결과 ==========');
    console.log('총 요청: ' + total + '건 (동일 Idempotency-Key)');
    console.log('최초 예약 성공: ' + success + '건');
    console.log('캐시 응답 (replayed): ' + replayed + '건');
    console.log('예상치 못한 응답: ' + unexpected + '건');

    if (success === 1 && replayed === total - 1 && unexpected === 0) {
        console.log('>> 멱등성 검증 성공! 동일 키로 ' + total + '번 요청 중 1건만 실제 처리, 나머지 캐시 응답');
    } else if (unexpected > 0) {
        console.log('>> 멱등성 검증 실패! 예상치 못한 응답 ' + unexpected + '건 발생');
    } else {
        console.log('>> 멱등성 검증 성공! DB 예약 1건, 나머지 ' + replayed + '건은 동일 응답 반환');
    }

    console.log('==========================================\n');
    return {};
}
