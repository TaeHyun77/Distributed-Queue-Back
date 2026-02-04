package com.example.integrated.queue.duplication

import com.example.integrated.util.Loggable
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Service
class DuplicationCheckService(
    private val duplicationCheckRepository: DuplicationCheckRepository
): Loggable {

    /* 요청 중복 확인
    * DB의 유니크 제약과 DuplicateKeyException을 통해 중복 요청을 감지
    * expiresAt을 설정하여 특정 기간마다 삭제될 수 있도록 함
    * */
    @Transactional
    suspend fun isDuplicate(
        queueType: String,
        userId: String,
        requestKey: String
    ): Boolean {

        val entity = DuplicationCheck(
            requestKey = requestKey,
            userId = userId,
            queueType = queueType,
            expiresAt = LocalDateTime.now().plusMinutes(5)
        )

        return try {
            duplicationCheckRepository.save(entity)
            false // 처음 요청
        } catch (e: DuplicateKeyException) {
            log.info { "중복된 요청입니다 - ${entity.requestKey}" }
            true  // 이미 처리됨
        }
    }
}