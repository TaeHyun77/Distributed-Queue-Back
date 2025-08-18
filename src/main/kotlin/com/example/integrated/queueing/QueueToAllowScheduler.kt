package com.example.integrated.queueing

import com.example.integrated.util.Loggable
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class QueueToAllowScheduler(
    private val queueService: QueueService,

    @Value("\${move.to.allow.interval}")
    private var moveToAllowInterval: Long
): CoroutineScope, Loggable {

    private val job = SupervisorJob()

    // CoroutineScope를 구현 : 클래스 내부에서 지정한 coroutineContext를 기반으로 coroutine을 호출할 수 있음
    // 스프링 빈으로 생명주기가 제어됨 : @Component를 지정하였기에
    // SupervisorJob() 사용 : 개별 작업 실패 시 전체 스코프가 중단되지 않도록
    override val coroutineContext = Dispatchers.IO + job

    // 일정 주기마다 특정 신호를 전달
    @OptIn(ObsoleteCoroutinesApi::class)
    private val tickerChannel = ticker(
        delayMillis = moveToAllowInterval,
        initialDelayMillis = 30000,
        mode = TickerMode.FIXED_DELAY
    )

    private val maxAllowedUsers = 3L
    private val queueTypes = listOf("reserve_공연A", "reserve_공연B", "reserve_공연C")

    @PostConstruct
    fun start() {
        launch {
            tickerChannel.consumeEach {
                try {
                    queueTypes.forEach { queueType ->
                        val count = queueService.allowUser(queueType, maxAllowedUsers)
                        log.info { "$queueType 허용열로 이동한 사용자 : $count" }
                    }
                } catch (e: Exception) {
                    log.error(e) { "스케줄러 실행 중 오류 발생" }
                }
            }
        }
    }

    @PreDestroy
    fun stop() {
        println("QueueToAllowScheduler 종료")

        tickerChannel.cancel() // 채널 정리
        job.cancel() // launch 포함 모든 자식 코루틴 정리
    }
}