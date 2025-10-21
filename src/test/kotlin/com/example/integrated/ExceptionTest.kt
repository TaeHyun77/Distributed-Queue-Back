package com.example.integrated

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.jupiter.api.Test

class ExceptionTest {

    @Test
    fun exceptionTest() {

        runBlocking {

            println("1번 : ${coroutineContext[Job]}")

            println("coroutine01 : start")

            launch {
                println("coroutine02 : start")
                delay(500)
                println("coroutine02 : end")
            }

            launch {
                println("coroutine03 : start")
                delay(500)
                println("coroutine03 : end")
            }

            supervisorScope {
                launch {
                    println("coroutine04 : start")

                    launch {
                        println("coroutine05 : start")
                        delay(500)
                        println("coroutine05 : end")
                    }

                    delay(200)
                    throw RuntimeException("coroutine04 exception")
                }
            }

            println("coroutine01 : end")
        }
    }
}