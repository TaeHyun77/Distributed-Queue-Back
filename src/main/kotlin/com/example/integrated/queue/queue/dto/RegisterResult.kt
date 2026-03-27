package com.example.integrated.queue.queue.dto

enum class RegisterResult {
    QUEUED,
    DIRECT_ALLOW,
    ALREADY_IN_WAIT,
    ALREADY_IN_ALLOW,
}