package com.example.integrated.queue.sse

import com.example.integrated.queue.queue.dto.QueueRequest
import com.example.integrated.util.Loggable
import kotlinx.coroutines.flow.Flow
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RequestMapping("/queue")
@RestController
class SseEventController(
    private val sseEventService: SseEventService
): Loggable {

    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamQueue(
        @RequestParam queueType: String,
        @RequestParam userId: String,
    ): Flow<ServerSentEvent<String>> {

        return sseEventService.streamQueueEvents(queueType, userId)
    }
}