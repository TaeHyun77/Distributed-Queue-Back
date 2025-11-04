package com.example.integrated.reserveException

enum class ErrorCode (
    val errorCode: String,
    val message: String
){
    UNKNOWN("000_UNKNOWN", "알 수 없는 에러 발생"),

    NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY("NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY", "Idempotency-Key 헤더 누락"),

    INVALID_QUEUE_CATEGORY("INVALID_QUEUE_CATEGORY", "유효하지 않은 QUEUE CATEGORY 입니다."),

    // QUEUEING
    ALREADY_REGISTERED_USER_IN_QUEUE("ALREADY_REGISTERED_USER", "이미 등록된 사용자입니다."),

    FAIL_TO_REGISTER_USER_IN_QUEUE("FAIL_TO_REGISTER_QUEUE", "대기열 등록에 실패하였습니다."),

    INTERNAL_ERROR("INTERNAL_ERROR", "대기열 예외 발생"),

    REDIS_OPERATION_FAILED("REDIS_OPERATION_FAILED", "Redis 작업 중 실패"),

    FAIL_TO_GENERATE_TOKEN("FAIL_TO_GENERATE_TOKEN", "토큰 생성 중 에러 발생")
}