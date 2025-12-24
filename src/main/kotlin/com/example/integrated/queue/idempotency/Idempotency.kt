package com.example.integrated.queue.idempotency

import com.example.integrated.BaseTime
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("queue_idempotency")
class Idempotency (

    val idempotencyKey: String,

    val userId: String,

    val queueType: String,

    val expiresAt: LocalDateTime
): BaseTime()