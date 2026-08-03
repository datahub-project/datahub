package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class PostgresTimeseriesAspectServiceConfiguration {

  @Bean
  @Nonnull
  @ConditionalOnProperty(
      prefix = "timeseriesAspectService",
      name = "implementation",
      havingValue = "postgres")
  public PostgresTimeseriesAspectService postgresTimeseriesAspectService(
      @Qualifier("pgTimeseriesEbeanServer") Database database,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      QueryFilterRewriteChain queryFilterRewriteChain,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Value("${postgres.pgTimeseries.pool.url:${ebean.url:}}") String timeseriesPoolUrl) {
    if (!postgresSqlSetupProperties.getPgTimeseries().isEnabled()) {
      throw new IllegalStateException(
          "timeseriesAspectService.implementation=postgres requires postgres.pgTimeseries.enabled=true");
    }
    if (timeseriesPoolUrl == null || timeseriesPoolUrl.isBlank()) {
      throw new IllegalStateException(
          "timeseriesAspectService.implementation=postgres but postgres.pgTimeseries.pool.url is empty");
    }
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(timeseriesPoolUrl.trim());
    if (info.databaseType != DatabaseType.POSTGRES) {
      throw new IllegalStateException(
          "timeseriesAspectService.implementation=postgres but postgres.pgTimeseries.pool.url is not PostgreSQL");
    }
    postgresSqlSetupProperties.applySqlSetupSchemaFromJdbcUrl(timeseriesPoolUrl);
    postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);

    return new PostgresTimeseriesAspectService(
        database,
        postgresSqlSetupProperties,
        configurationProvider.getTimeseriesAspectService(),
        queryFilterRewriteChain,
        entityRegistry);
  }
}
