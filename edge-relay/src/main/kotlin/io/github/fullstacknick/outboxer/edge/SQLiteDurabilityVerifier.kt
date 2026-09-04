package io.github.fullstacknick.outboxer.edge

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class SQLiteDurabilityVerifier(private val jdbcTemplate: JdbcTemplate) : ApplicationRunner {
    override fun run(args: ApplicationArguments) {
        val journalMode = jdbcTemplate.queryForObject("PRAGMA journal_mode", String::class.java)?.lowercase()
        val synchronous = jdbcTemplate.queryForObject("PRAGMA synchronous", Int::class.java)
        val foreignKeys = jdbcTemplate.queryForObject("PRAGMA foreign_keys", Int::class.java)
        check(journalMode == "wal") { "SQLite journal_mode must be WAL but was $journalMode" }
        check(synchronous == 2) { "SQLite synchronous must be FULL (2) but was $synchronous" }
        check(foreignKeys == 1) { "SQLite foreign_keys must be enabled" }
    }
}
