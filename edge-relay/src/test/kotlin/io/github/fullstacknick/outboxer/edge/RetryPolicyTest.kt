package io.github.fullstacknick.outboxer.edge

import java.time.Duration
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RetryPolicyTest {
    @Test
    fun `full jitter stays within exponential cap`() {
        val policy = RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(60), Random(42))

        assertThat(policy.delay(0)).isBetween(Duration.ofMillis(1), Duration.ofSeconds(1))
        assertThat(policy.delay(8)).isBetween(Duration.ofMillis(1), Duration.ofSeconds(60))
        assertThat(policy.delay(30)).isBetween(Duration.ofMillis(1), Duration.ofSeconds(60))
    }
}
