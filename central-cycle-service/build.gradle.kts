plugins {
    java
    alias(libs.plugins.spring.boot)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val integrationTestSourceSet = sourceSets.create("integrationTest") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

configurations[integrationTestSourceSet.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":contracts"))
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation(libs.hivemq.client)
    implementation(libs.springdoc.webflux.ui)
    implementation("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("org.postgresql:r2dbc-postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    add(integrationTestSourceSet.implementationConfigurationName, platform(libs.testcontainers.bom))
    add(integrationTestSourceSet.implementationConfigurationName, libs.testcontainers.postgresql)
    add(integrationTestSourceSet.runtimeOnlyConfigurationName, "org.junit.platform:junit-platform-launcher")
}

tasks.register<Test>("integrationTest") {
    description = "Runs central persistence tests against an ephemeral PostgreSQL container."
    group = "verification"
    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath
    shouldRunAfter(tasks.test)
}
