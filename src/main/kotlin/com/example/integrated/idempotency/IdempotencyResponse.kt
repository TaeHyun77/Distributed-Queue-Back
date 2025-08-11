package com.example.integrated.idempotency

data class IdempotencyResponse (

    val statusCode: Int,

    val responseBody: String? = null,

)