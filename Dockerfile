FROM amazoncorretto:17 AS builder

WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts settings.gradle.kts ./

RUN chmod +x ./gradlew && ./gradlew dependencies --no-daemon || true

COPY src src
RUN ./gradlew clean bootJar -x test --no-daemon

# ===== 실행 스테이지 =====
FROM amazoncorretto:17-alpine

RUN apk add --no-cache curl

WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]