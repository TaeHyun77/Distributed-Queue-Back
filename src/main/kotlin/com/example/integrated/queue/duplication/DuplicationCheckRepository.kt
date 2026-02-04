package com.example.integrated.queue.duplication

import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface DuplicationCheckRepository: CoroutineCrudRepository<DuplicationCheck, Long> {

}