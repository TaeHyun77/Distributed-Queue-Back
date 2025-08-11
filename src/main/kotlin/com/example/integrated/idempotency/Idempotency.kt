package com.example.integrated.idempotency

import com.example.integrated.BaseTime
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import java.time.LocalDateTime

@Entity
class Idempotency(

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    // 멱등키 값
    @Column(unique = true)
    val idempotencyKey: String,

    // 요청 url
    @Column(nullable = false)
    val url: String,

    // HTTP 요청 메서드
    @Column(nullable = false)
    val httpMethod: String,

    val statusCode: Int,

    // 응답 값
    @Column(columnDefinition = "TEXT")
    val responseBody: String? = null,

    // 멱등키 유효 기간 ( 10분으로 설정함 )
    val expires_at: LocalDateTime

): BaseTime()