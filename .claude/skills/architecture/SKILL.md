---
name: architecture
description: 대기열 시스템 아키텍처 및 Redis 키 구조
---
# 아키텍처

트래픽 흐름: 사용자 요청 → Nginx → Lua 스크립트로 원자적 대기열/참가열 삽입 → Redis Pub/Sub → SSE 알림

## Redis 키 구조
- 대기열: `{queueType}:user-queue:wait` — Sorted Set, score = 이벤트별 단조 증가 시퀀스 (`INCR queue:seq:{queueType}` 결과)
- 참가열: `{queueType}:user-queue:allow` — Sorted Set, score = 만료 타임스탬프
- 시퀀스 카운터: `queue:seq:{queueType}` — INCR 카운터 (영구 보존, 대기열 신규 진입 시마다 +1)
- 활성 큐 플래그: `active-allow-queue` — 스케줄링 활성 큐 타입 추적
- Pub/Sub 채널: `queueing_system`

## 분산 안전성
- Redisson 분산 락(`scheduling_key`, 4초 lease)으로 스케줄러 단일 실행 보장
- Lua 스크립트(`enqueue-or-allow.lua`, `schedule-promote.lua`)로 Redis 연산 원자성 보장

## Docker Compose 서비스 구성
- 앱: queueing01/02/03 (WebFlux, 포트 8081)
- Redis: master + slave×2 + sentinel×3
- 로드밸런서: Nginx (라운드로빈)
- 모니터링: Prometheus + Grafana
- 예약: reserve01/02 + MySQL (별도 프로젝트 통합)
