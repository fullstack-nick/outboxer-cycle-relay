FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY gradle gradle
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY contracts contracts
COPY machine-simulator machine-simulator
COPY edge-relay/build.gradle.kts edge-relay/build.gradle.kts
COPY central-cycle-service/build.gradle.kts central-cycle-service/build.gradle.kts
RUN --mount=type=cache,target=/root/.gradle chmod +x gradlew && ./gradlew :machine-simulator:installDist --no-daemon

FROM eclipse-temurin:21-jre-jammy
RUN groupadd --system --gid 10001 outboxer && useradd --system --uid 10001 --gid outboxer --home-dir /nonexistent --shell /usr/sbin/nologin outboxer
WORKDIR /opt/outboxer
COPY --from=build --chown=outboxer:outboxer /workspace/machine-simulator/build/install/machine-simulator/ ./
USER 10001:10001
ENTRYPOINT ["/opt/outboxer/bin/machine-simulator"]
