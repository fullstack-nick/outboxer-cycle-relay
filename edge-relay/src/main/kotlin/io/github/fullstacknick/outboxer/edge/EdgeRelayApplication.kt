package io.github.fullstacknick.outboxer.edge

import io.github.fullstacknick.outboxer.contract.ContractValidator
import java.time.Clock
import java.time.Duration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import tools.jackson.databind.json.JsonMapper

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
class EdgeRelayApplication {
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun contractValidator(mapper: JsonMapper, clock: Clock, properties: EdgeProperties): ContractValidator =
        ContractValidator(mapper, clock, Duration.ofSeconds(properties.futureClockToleranceSeconds))
}

fun main(args: Array<String>) {
    runApplication<EdgeRelayApplication>(*args)
}
