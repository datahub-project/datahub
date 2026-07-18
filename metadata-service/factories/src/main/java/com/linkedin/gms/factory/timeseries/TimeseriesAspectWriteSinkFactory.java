package com.linkedin.gms.factory.timeseries;

import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectDao;
import com.linkedin.metadata.timeseries.write.TimeseriesAspectWriteSink;
import com.linkedin.metadata.timeseries.write.postgres.PostgresTimeseriesAspectWriteSink;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class TimeseriesAspectWriteSinkFactory {

  @Bean
  @Nonnull
  public TimeseriesAspectWriteSink timeseriesAspectWriteSink(
      @Qualifier("pgTimeseriesEbeanServer") ObjectProvider<Database> pgTimeseriesDatabaseProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Value("${postgres.pgTimeseries.pool.url:${ebean.url:}}") String timeseriesPoolUrl,
      @Value("${timeseriesAspectService.implementation:elasticsearch}")
          String timeseriesImplementation) {
    if ("postgres".equalsIgnoreCase(timeseriesImplementation.trim())) {
      return TimeseriesAspectWriteSink.NOOP;
    }
    if (!postgresSqlSetupProperties.getPgTimeseries().isEnabled()) {
      return TimeseriesAspectWriteSink.NOOP;
    }
    Database database = pgTimeseriesDatabaseProvider.getIfAvailable();
    if (database == null) {
      log.warn(
          "postgres.pgTimeseries.enabled but pgTimeseriesEbeanServer is not available; skipping PostgreSQL timeseries dual-write");
      return TimeseriesAspectWriteSink.NOOP;
    }
    if (timeseriesPoolUrl == null || timeseriesPoolUrl.isBlank()) {
      log.warn(
          "postgres.pgTimeseries.enabled but postgres.pgTimeseries.pool.url is empty; skipping PostgreSQL timeseries dual-write");
      return TimeseriesAspectWriteSink.NOOP;
    }
    try {
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(timeseriesPoolUrl.trim());
      if (info.databaseType != DatabaseType.POSTGRES) {
        log.warn(
            "postgres.pgTimeseries.enabled but postgres.pgTimeseries.pool.url is not PostgreSQL; skipping PostgreSQL timeseries dual-write");
        return TimeseriesAspectWriteSink.NOOP;
      }
      postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(timeseriesPoolUrl);
      postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    } catch (IllegalArgumentException | IllegalStateException e) {
      log.warn(
          "postgres.pgTimeseries.enabled but Postgres SqlSetup validation failed ({}); skipping PostgreSQL timeseries dual-write",
          e.getMessage());
      return TimeseriesAspectWriteSink.NOOP;
    }
    return new PostgresTimeseriesAspectWriteSink(
        new PostgresTimeseriesAspectDao(database, postgresSqlSetupProperties));
  }
}
