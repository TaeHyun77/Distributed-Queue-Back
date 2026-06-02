---
name: benchmark
description: 예약 부하 벤치마크 테스트 실행 및 분석
---
# 벤치마크 테스트

## 실행 방법
cd reserve-test && bash reserve-benchmark-test.sh

## 테스트 흐름 (VU 단계별 반복)
1. docker-compose up 및 헬스체크
2. loadtest1 로그인 → init/load-test → scheduleId 확보
3. 웜업 (VUS=10) 후 데이터 리셋
4. 본 테스트 (k6 setup에서 N명 배치 로그인 → default에서 N건 동시 예약)
5. 결과 파싱 (성공/실패/응답시간) → docker-compose down

## 핵심 파일
- reserve-test/reserve-benchmark-test.sh — 오케스트레이션
- reserve-test/reserve-load-test.js — k6 테스트 스크립트

## 벤치마크 결과 기준
- 성공률 100% 미만 → 한계 도달
- p99가 이전 단계 대비 2배 이상 급등 → 한계 도달
- 한계 직전 VU를 권장 max-capacity로 제시
