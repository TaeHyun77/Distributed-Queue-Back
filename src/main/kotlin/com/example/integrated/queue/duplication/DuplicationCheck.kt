package com.example.integrated.queue.duplication

import com.example.integrated.BaseTime
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("duplication_check")
class DuplicationCheck (

    @Id
    val id: Long? = null,

    // Database Level 에서 idempotencyKey의 유니크 제약 조건을 설정
    val requestKey: String,

    val userId: String,

    val queueType: String,

    val expiresAt: LocalDateTime
): BaseTime()