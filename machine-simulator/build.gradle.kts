import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    application
    alias(libs.plugins.kotlin.jvm)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_21
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

application {
    mainClass = "io.github.fullstacknick.outboxer.simulator.SimulatorApplicationKt"
}

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":contracts"))
    implementation(libs.hivemq.client)
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.slf4j:slf4j-api")
    runtimeOnly("ch.qos.logback:logback-classic")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
