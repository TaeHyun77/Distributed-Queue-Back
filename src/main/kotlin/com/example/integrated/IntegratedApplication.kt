package com.example.integrated

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
import org.springframework.scheduling.annotation.EnableScheduling

@EnableR2dbcRepositories
@EnableR2dbcAuditing
@EnableScheduling
@SpringBootApplication
class IntegratedApplication

fun main(args: Array<String>) {
	runApplication<IntegratedApplication>(*args)
}
