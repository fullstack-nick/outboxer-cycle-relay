plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    api(platform(libs.spring.boot.dependencies))
    api("tools.jackson.core:jackson-databind")
    api(libs.json.schema.validator)

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
}

sourceSets {
    main {
        resources {
            srcDir(layout.projectDirectory)
            include("*.schema.json")
            include("examples/**")
        }
    }
}

