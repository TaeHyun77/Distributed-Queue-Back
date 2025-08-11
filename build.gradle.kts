plugins {
	kotlin("jvm") version "1.9.25"
	kotlin("plugin.spring") version "1.9.25"
	id("org.springframework.boot") version "3.5.4"
	id("io.spring.dependency-management") version "1.1.7"
	id("org.jetbrains.kotlin.plugin.jpa") version "1.9.22"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(17)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-webflux")

	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("io.projectreactor:reactor-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	implementation("jakarta.servlet:jakarta.servlet-api:6.0.0")

	implementation("org.springframework.boot:spring-boot-starter-data-jpa")

	implementation("org.springframework.boot:spring-boot-starter-security")


	// Mysql 의존성
	implementation("mysql:mysql-connector-java:8.0.33")

	// log 의존성
	implementation("io.github.oshai:kotlin-logging-jvm:5.1.4")

	// kafka 의존성
	implementation ("org.springframework.kafka:spring-kafka")

	// 코루틴
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.3")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.6.3")

	// serializer
	implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.3")

	implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
