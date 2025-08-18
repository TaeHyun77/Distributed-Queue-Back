package com.example.integrated.idempotency

import com.example.integrated.BaseTime
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("idempotency")
data class Idempotency(

    @Id
    val id: Long? = null,

    // 멱등키 값
    val idempotencyKey: String,

    // 요청 url
    val url: String,

    // HTTP 요청 메서드
    val httpMethod: String,

    val statusCode: Int,

    // 응답 값
    val responseBody: String? = null,

    // 멱등키 유효 기간 ( 10분으로 설정함 )
    val expires_at: LocalDateTime
): BaseTime()