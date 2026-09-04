package io.github.fullstacknick.outboxer.edge

import java.time.Duration
import kotlin.math.min
import kotlin.random.Random

class RetryPolicy(
    private val base: Duration,
    private val cap: Duration,
    private val random: Random = Random.Default,
) {
    fun delay(attemptCount: Int): Duration {
        val exponent = attemptCount.coerceIn(0, 20)
        val maximum = min(cap.toMillis(), base.toMillis() * (1L shl exponent))
        return Duration.ofMillis(if (maximum <= 1) maximum else random.nextLong(1, maximum + 1))
    }
}
