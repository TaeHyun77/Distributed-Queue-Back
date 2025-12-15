package com.example.integrated.queue.queue

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.Loggable
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class QueueToAllowScheduler(
    private val queueService: QueueService,

    @Value("\${move.to.allow.interval}")
    private var moveToAllowInterval: Long,

    private val redisLockUtil: RedisLockUtil
): Loggable {

    /*
    * channel 방식에서 flow 방식으로 변경
    *
    * period : 스케줄러의 실행 주기
    * initialDelay : 애플리케이션 시작 후 스케줄링이 실행될 때까지의 대기 시간
    *
    * 애플리케이션 실행 후 schedulingStart()에서 tickerFlow가 호출되면 무한 루프가 돌아가며 period 주기로 신호를 발행하면, collect로 받아서 작업 실행
    * */
    fun tickerFlow(period: Long, initialDelay: Long = 0) = flow {
        delay(initialDelay)

        while (true) {
            emit(Unit)

            delay(period)
        }
    }

    val tickerScope = CoroutineScope(Dispatchers.Default)
    private val maxAllowedUsers = 3L
    private val queueTypes = listOf("reserve_공연A", "reserve_공연B", "reserve_공연C")

    @PostConstruct
    fun schedulingStart() {
        tickerScope.launch {
            tickerFlow(moveToAllowInterval, 30_000) // 30초 후 시작
                .collect {
                    val result = redisLockUtil.acquireLockAndRun("scheduling_key") {
                        queueTypes.forEach { queueType ->
                            try {
                                val count = queueService.allowUser(queueType, maxAllowedUsers)

                                log.info { "$queueType 허용열로 이동한 사용자 : $count" }
                            } catch (e: Exception) {
                                log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
                            }
                        }
                    }

                    if (result == null) {
                        log.debug { "다른 인스턴스가 스케줄링을 실행 중입니다." }
                    }
                }
        }
    }

    @PreDestroy
    fun stop() {
        println("스케줄링 종료")

        tickerScope.cancel()
    }
}