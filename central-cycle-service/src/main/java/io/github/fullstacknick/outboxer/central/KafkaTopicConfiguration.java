package io.github.fullstacknick.outboxer.central;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaTopicConfiguration {

    static final String CYCLE_EVENTS = "cycle-events";
    static final String INVALID_EVENTS = "cycle-events-invalid";

    @Bean
    NewTopic cycleEventsTopic() {
        return TopicBuilder.name(CYCLE_EVENTS).partitions(12).replicas(1).build();
    }

    @Bean
    NewTopic invalidEventsTopic() {
        return TopicBuilder.name(INVALID_EVENTS).partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        // A database outage must hold the partition at the uncommitted record;
        // poison data is isolated explicitly by the listener before it returns.
        return new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
