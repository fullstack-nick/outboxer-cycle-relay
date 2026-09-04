package io.github.fullstacknick.outboxer.edge

import io.micrometer.core.instrument.MeterRegistry
import java.nio.file.Files
import java.nio.file.Path
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.stereotype.Component

@Component("storage")
class EdgeStorageHealthIndicator(
    @Value("\${spring.datasource.url}") databaseUrl: String,
    private val properties: EdgeProperties,
    registry: MeterRegistry,
) : HealthIndicator {
    private val storagePath = sqlitePath(databaseUrl)

    init {
        registry.gauge("edge.storage.usable.bytes", this) { it.usableBytes().toDouble() }
    }

    override fun health(): Health {
        val usable = usableBytes()
        val parent = storagePath.parent
        val writable = parent != null && Files.isWritable(parent)
        val details = mapOf(
            "usableBytes" to usable,
            "minimumFreeBytes" to properties.minimumFreeBytes,
            "writable" to writable,
        )
        return if (writable && usable >= properties.minimumFreeBytes) {
            Health.up().withDetails(details).build()
        } else {
            Health.status("DEGRADED").withDetails(details).build()
        }
    }

    private fun usableBytes(): Long = runCatching {
        Files.getFileStore(storagePath.parent ?: storagePath).usableSpace
    }.getOrDefault(0)

    private fun sqlitePath(url: String): Path {
        require(url.startsWith("jdbc:sqlite:") && !url.contains(":memory:")) {
            "A file-backed SQLite URL is required for edge durability"
        }
        return Path.of(url.removePrefix("jdbc:sqlite:").substringBefore('?')).toAbsolutePath().normalize()
    }
}
