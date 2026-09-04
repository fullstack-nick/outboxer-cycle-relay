import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
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

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":contracts"))
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation(libs.hivemq.client)
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly(libs.sqlite.jdbc)

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.awaitility:awaitility")
    testImplementation(libs.sqlite.jdbc)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.named<BootJar>("bootJar") {
    // The driver extracts a platform native library at runtime. Spring Boot's
    // nested-jar loader must unpack this dependency before SQLite can find it.
    requiresUnpack("**/sqlite-jdbc-*.jar")
}
