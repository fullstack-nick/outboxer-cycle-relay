package io.github.fullstacknick.outboxer.central;

import io.github.fullstacknick.outboxer.contract.ContractValidator;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafka
@EnableScheduling
public class CentralCycleServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CentralCycleServiceApplication.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    ContractValidator contractValidator(JsonMapper mapper, Clock clock, CentralProperties properties) {
        return new ContractValidator(mapper, clock, Duration.ofSeconds(properties.getFutureClockToleranceSeconds()));
    }
}
