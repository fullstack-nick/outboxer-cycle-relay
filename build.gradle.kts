import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.Exec

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
    format("source") {
        target("**/*.java", "**/*.kt")
        targetExclude("**/build/**")
        trimTrailingWhitespace()
        endWithNewline()
    }
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
    dependsOn(":central-cycle-service:integrationTest")
}

tasks.register("contractCompatibilityTest") {
    group = "verification"
    description = "Runs the version 1 and version 2 contract compatibility suite."
    dependsOn(":contracts:test")
}

fun registerAcceptanceTask(name: String, mode: String, taskDescription: String) = tasks.register<Exec>(name) {
    group = "verification"
    description = taskDescription
    val javaBinary = file(
        "${System.getProperty("java.home")}/bin/java${if (System.getProperty("os.name").startsWith("Windows")) ".exe" else ""}",
    )
    workingDir(rootDir)
    commandLine(javaBinary, file("scripts/AcceptanceCoordinator.java"), mode)
}

registerAcceptanceTask("composeSmokeTest", "smoke", "Runs the local Docker Compose smoke test.").configure {
    dependsOn("check")
}

registerAcceptanceTask("crashWindowTest", "crash", "Runs deterministic edge and central crash-window tests.").configure {
    dependsOn("check")
}

registerAcceptanceTask("loadTest", "load", "Runs the sustained local throughput test.").configure {
    dependsOn("check")
}

registerAcceptanceTask("outageTest", "outage", "Runs the full local outage and recovery acceptance test.").configure {
    dependsOn("check")
}

tasks.register("publicationCheck") {
    group = "verification"
    description = "Checks tracked content and repository metadata before publication."
    dependsOn("check")
}
