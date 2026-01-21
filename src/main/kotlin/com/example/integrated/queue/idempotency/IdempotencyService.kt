package com.example.integrated.queue.idempotency

import com.example.integrated.util.Loggable
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class IdempotencyService(
    private val idempotencyRepository: IdempotencyRepository
): Loggable {

    /* 멱등 로직 확인
     * 키 값으로 조회하여 있으면 false 반환하는 방법은 동시성 문제가 발생할 수 있으므로 아래와 같이 설정
     * expiresAt을 설정하여 특정 기간마다 삭제하여 정리하도록 함
     * */
    @Transactional
    suspend fun checkAndSaveIdempotencyKey(
        queueType: String,
        userId: String,
        idempotencyKey: String
    ): Boolean {

        val entity = Idempotency(
            idempotencyKey = idempotencyKey,
            userId = userId,
            queueType = queueType,
            expiresAt = LocalDateTime.now().plusMinutes(5)
        )

        return try {
            idempotencyRepository.save(entity)
            false
        } catch (e: DuplicateKeyException) {
            log.info { "중복된 요청입니다 - ${entity.idempotencyKey}" }
            true
        } catch (e: DataIntegrityViolationException) {
            log.info { "중복된 요청입니다 - ${entity.idempotencyKey}" }
            true
        }
    }
}