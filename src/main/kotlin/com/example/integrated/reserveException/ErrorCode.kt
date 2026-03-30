package com.example.integrated.reserveException

enum class ErrorCode (
    val errorCode: String,
    val message: String
){
    UNKNOWN("000_UNKNOWN", "알 수 없는 에러 발생"),

    NOT_EXIST_IN_HEADER_REQUEST_KEY("NOT_EXIST_IN_HEADER_REQUEST_KEY", "REQUEST_KEY가 Header에 존재하지 않습니다."),

    FAIL_TO_GENERATE_TOKEN("FAIL_TO_GENERATE_TOKEN", "토큰 생성 중 에러 발생"),

    FAILED_TO_STORE_TOKEN_IN_COOKIE("FAILED_TO_STORE_TOKEN_IN_COOKIE", "쿠키에 토큰 저장 실패")
}