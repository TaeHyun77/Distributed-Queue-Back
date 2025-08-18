package com.example.integrated.util

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging

// log 사용을 위한 인터페이스
interface Loggable {
    val log: KLogger
        get() = KotlinLogging.logger(this::class.java.simpleName)
}