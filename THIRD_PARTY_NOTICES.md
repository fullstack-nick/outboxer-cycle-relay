# Third-party notices

Outboxer source is licensed under the MIT License. The project builds on third-party software, each of which remains governed by its own license and copyright notices. Inclusion here does not imply authorship, sponsorship, or endorsement by those projects.

## Principal JVM components

| Component or family | License identifier |
| --- | --- |
| Spring Boot, Spring Framework, Spring for Apache Kafka, Spring Data R2DBC | Apache-2.0 |
| Kotlin compiler, standard library, and Gradle plugin | Apache-2.0 |
| HiveMQ MQTT Client | Apache-2.0 |
| NetworkNT JSON Schema Validator | Apache-2.0 |
| Xerial SQLite JDBC | Apache-2.0 |
| Jackson | Apache-2.0 |
| Reactor | Apache-2.0 |
| Micrometer | Apache-2.0 |
| Flyway Community | Apache-2.0 |
| R2DBC PostgreSQL and R2DBC SPI | Apache-2.0 |
| PostgreSQL JDBC Driver | BSD-2-Clause |
| HikariCP | Apache-2.0 |
| Netty | Apache-2.0 |
| springdoc-openapi | Apache-2.0 |
| JUnit Jupiter | EPL-2.0 |
| AssertJ | Apache-2.0 |
| Awaitility | Apache-2.0 |
| Testcontainers for Java | MIT |
| SLF4J | MIT |
| Logback | EPL-1.0 OR LGPL-2.1-only |
| Gradle wrapper and tooling | Apache-2.0 |

This table summarizes the principal direct dependencies and major runtime families. The authoritative version inventory for the checked-out source is generated from all resolvable JVM runtime and test configurations:

```bash
./gradlew licenseReport
```

The result is written to `build/reports/licenses/resolved-dependencies.txt`. Dependency versions are locked per module, and downloaded Gradle artifacts are checked against `gradle/verification-metadata.xml`.

## Runtime images and local tools

| Component | Repository version | License notes |
| --- | --- | --- |
| Apache Kafka image | `apache/kafka:4.3.1` | Apache-2.0 |
| Eclipse Mosquitto image | `eclipse-mosquitto:2.1.2-alpine` | EPL-2.0 and EDL-1.0 project licensing; bundled packages retain their licenses |
| PostgreSQL image | `postgres:18.6-alpine` | PostgreSQL License; bundled packages retain their licenses |
| Eclipse Temurin JDK/JRE images | digest-pinned `21-*-jammy` images | OpenJDK GPL-2.0-only WITH Classpath-exception-2.0; image packages retain their licenses |
| kind | `v0.33.0` | Apache-2.0 |
| Kubernetes and cached schemas | `v1.36.1` | Apache-2.0 |
| kubeconform | `v0.8.0` | Apache-2.0 |

Container images and optional test tools are fetched by Docker or local scripts; they are not vendored in this repository. The kind and kubeconform downloads are verified against their published SHA-256 files.

## Review notes

- The application does not copy third-party source into its modules.
- The Gradle wrapper JAR is included so the pinned build can start consistently.
- Transitive components remain subject to the notice and source requirements in their own distributions.
- Before updating a library, image, or tool, regenerate the inventory, inspect the resolved graph, review the upstream license, and update this file when the principal set or licensing terms differ.
