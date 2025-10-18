package com.example.integrated

import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class ExceptionTest {

    @Test
    fun exceptionTest() {

        runBlocking {

            val supervisorJob = SupervisorJob()

            println("coroutine01 : start")

            launch(supervisorJob) {
                println("coroutine02 : start")
                delay(500)
                println("coroutine02 : end")
            }

            launch(supervisorJob) {
                println("coroutine03 : start")
                delay(500)
                println("coroutine03 : end")
            }

            launch(supervisorJob) {
                println("coroutine04 : start")

                launch {
                    println("coroutine05 : start")
                    delay(500)
                    println("coroutine05 : end")
                }

                delay(200)
                throw RuntimeException("coroutine04 exception")
            }

            delay(500)
            println("coroutine01 : end")
        }
    }
}