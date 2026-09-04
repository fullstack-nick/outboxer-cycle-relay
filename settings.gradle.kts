pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
    resolutionStrategy {
        eachPlugin {
            when (requested.id.id) {
                "org.springframework.boot" -> useModule("org.springframework.boot:spring-boot-gradle-plugin:${requested.version}")
                "org.jetbrains.kotlin.jvm" -> useModule("org.jetbrains.kotlin:kotlin-gradle-plugin:${requested.version}")
                "org.jetbrains.kotlin.plugin.spring" -> useModule("org.jetbrains.kotlin:kotlin-allopen:${requested.version}")
                "com.diffplug.spotless" -> useModule("com.diffplug.spotless:spotless-plugin-gradle:${requested.version}")
            }
        }
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

rootProject.name = "outboxer-cycle-relay"

include("contracts")
include("machine-simulator")
include("edge-relay")
include("central-cycle-service")
