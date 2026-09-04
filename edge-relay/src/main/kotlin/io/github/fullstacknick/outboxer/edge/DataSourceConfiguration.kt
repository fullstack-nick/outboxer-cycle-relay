package io.github.fullstacknick.outboxer.edge

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.nio.file.Files
import java.nio.file.Path
import javax.sql.DataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DataSourceConfiguration {
    @Bean(destroyMethod = "close")
    fun dataSource(@Value("\${spring.datasource.url}") url: String): DataSource {
        createParentDirectory(url)
        val configuration = HikariConfig().apply {
            jdbcUrl = url
            driverClassName = "org.sqlite.JDBC"
            // SQLite supports concurrent readers but has one writer. A single
            // connection prevents competing outbox, receipt, and retry writes
            // from turning normal load into SQLITE_BUSY retry storms.
            maximumPoolSize = 1
            minimumIdle = 1
            connectionTimeout = 30_000
            poolName = "edge-sqlite"
        }
        return HikariDataSource(configuration)
    }

    private fun createParentDirectory(url: String) {
        if (!url.startsWith("jdbc:sqlite:") || url.contains(":memory:")) return
        val filename = url.removePrefix("jdbc:sqlite:").substringBefore('?')
        val path = Path.of(filename).toAbsolutePath().normalize()
        path.parent?.let(Files::createDirectories)
    }
}
