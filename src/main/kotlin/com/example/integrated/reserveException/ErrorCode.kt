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

    NOT_EXIST_TTL_INFO("NOT_EXIST_TTL_INFO", "TTL 값이 존재하지 않습니다."),

    REDIS_FAILED_TO_ACQUIRED_LOCK("REDIS_FAILED_TO_ACQUIRED_LOCK", "Lock 획득 실패")
}