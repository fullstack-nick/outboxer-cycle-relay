package io.github.fullstacknick.outboxer.central;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
class JdbcConfiguration {

    @Bean(destroyMethod = "close")
    DataSource centralDataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password) {
        HikariConfig configuration = new HikariConfig();
        configuration.setJdbcUrl(url);
        configuration.setUsername(username);
        configuration.setPassword(password);
        configuration.setMaximumPoolSize(8);
        configuration.setMinimumIdle(1);
        configuration.setPoolName("central-postgres");
        return new HikariDataSource(configuration);
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource centralDataSource) {
        return new JdbcTemplate(centralDataSource);
    }
}
