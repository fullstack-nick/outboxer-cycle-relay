FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY gradle gradle
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY contracts contracts
COPY central-cycle-service central-cycle-service
COPY machine-simulator/build.gradle.kts machine-simulator/build.gradle.kts
COPY edge-relay/build.gradle.kts edge-relay/build.gradle.kts
RUN --mount=type=cache,target=/root/.gradle chmod +x gradlew && ./gradlew :central-cycle-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 outboxer \
    && useradd --system --uid 10001 --gid outboxer --home-dir /nonexistent --shell /usr/sbin/nologin outboxer
WORKDIR /opt/outboxer
COPY --from=build --chown=outboxer:outboxer /workspace/central-cycle-service/build/libs/central-cycle-service-*.jar app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/outboxer/app.jar"]

