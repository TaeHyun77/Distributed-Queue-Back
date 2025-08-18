package com.example.integrated.idempotency

import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Mono

interface IdempotencyRepository: ReactiveCrudRepository<Idempotency, Long> {

    fun findByIdempotencyKey(idempotencyKey: String): Mono<Idempotency>

}