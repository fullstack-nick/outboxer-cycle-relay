import org.gradle.api.tasks.testing.Test

plugins {
    base
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.spotless)
}

group = "io.github.fullstacknick.outboxer"
version = "1.0.0-SNAPSHOT"

allprojects {
    group = rootProject.group
    version = rootProject.version
}

subprojects {
    configurations.configureEach {
        resolutionStrategy.activateDependencyLocking()
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("failed", "skipped")
        }
    }
}

spotless {
    format("repository") {
        target("*.kts", "*.md", "*.yml", "*.yaml", "*.json", "*.sh", "*.ps1")
        targetExclude(
            "INTERNAL_PROJECT_PLAN.md",
            "gradle/verification-metadata.xml",
            "docs/evidence/**",
        )
        trimTrailingWhitespace()
        endWithNewline()
    }
}

tasks.named("check") {
    dependsOn(subprojects.map { "${it.path}:check" })
    dependsOn("spotlessCheck")
}

tasks.register("integrationTest") {
    group = "verification"
    description = "Runs all component integration tests."
    dependsOn(subprojects.mapNotNull { project ->
        project.tasks.findByName("integrationTest")?.path
    })
}

tasks.register("contractCompatibilityTest") {
    group = "verification"
    description = "Runs the version 1 and version 2 contract compatibility suite."
    dependsOn(":contracts:test")
}

tasks.register("composeSmokeTest") {
    group = "verification"
    description = "Runs the local Docker Compose smoke test."
    dependsOn("check")
    doLast {
        logger.lifecycle("Compose smoke coordinator will be implemented before the Stage 2 gate.")
    }
}

tasks.register("outageTest") {
    group = "verification"
    description = "Runs the full local outage and recovery acceptance test."
    doLast {
        throw GradleException("Outage acceptance coordinator is intentionally failing until implemented.")
    }
}

tasks.register("publicationCheck") {
    group = "verification"
    description = "Checks tracked content and repository metadata before publication."
    dependsOn("check")
}
