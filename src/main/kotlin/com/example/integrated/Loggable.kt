package com.example.integrated

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.jvm.java

// log 사용을 위한 인터페이스
interface Loggable {
    val log: KLogger
        get() = KotlinLogging.logger(this::class.java.simpleName)
}