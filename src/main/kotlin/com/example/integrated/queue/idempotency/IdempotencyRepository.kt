package com.example.integrated.queue.idempotency

import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface IdempotencyRepository: CoroutineCrudRepository<Idempotency, Long> {

    suspend fun findByIdempotencyKey(idempotencyKey: String): Idempotency?

}