FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY gradle gradle
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY contracts contracts
COPY edge-relay edge-relay
COPY machine-simulator/build.gradle.kts machine-simulator/build.gradle.kts
COPY central-cycle-service/build.gradle.kts central-cycle-service/build.gradle.kts
RUN --mount=type=cache,target=/root/.gradle chmod +x gradlew && ./gradlew :edge-relay:bootJar --no-daemon
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) sqlite_arch=x86_64 ;; \
      aarch64) sqlite_arch=aarch64 ;; \
      *) echo "Unsupported image architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    mkdir -p /workspace/sqlite-native; \
    cd /workspace/sqlite-native; \
    jar xf /workspace/edge-relay/build/libs/edge-relay-1.0.0-SNAPSHOT.jar BOOT-INF/lib/sqlite-jdbc-3.53.4.0.jar; \
    jar xf BOOT-INF/lib/sqlite-jdbc-3.53.4.0.jar "org/sqlite/native/Linux/${sqlite_arch}/libsqlitejdbc.so"; \
    cp "org/sqlite/native/Linux/${sqlite_arch}/libsqlitejdbc.so" libsqlitejdbc.so

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 outboxer \
    && useradd --system --uid 10001 --gid outboxer --home-dir /nonexistent --shell /usr/sbin/nologin outboxer \
    && mkdir -p /var/lib/outboxer \
    && chown 10001:10001 /var/lib/outboxer
WORKDIR /opt/outboxer
COPY --from=build --chown=outboxer:outboxer /workspace/edge-relay/build/libs/edge-relay-*.jar app.jar
COPY --from=build --chown=outboxer:outboxer /workspace/sqlite-native/libsqlitejdbc.so /opt/outboxer/native/libsqlitejdbc.so
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-Dorg.sqlite.lib.path=/opt/outboxer/native", "-Dorg.sqlite.lib.name=libsqlitejdbc.so", "-jar", "/opt/outboxer/app.jar"]
