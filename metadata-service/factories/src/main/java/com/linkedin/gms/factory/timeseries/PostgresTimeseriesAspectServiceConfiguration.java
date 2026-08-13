package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class PostgresTimeseriesAspectServiceConfiguration {

  @Bean
  @Nonnull
  @Conditional(TimeseriesPostgresBackendCondition.class)
  public PostgresTimeseriesAspectService postgresTimeseriesAspectService(
      // ObjectProvider avoids an opaque UnsatisfiedDependencyException when the registry bean is
      // absent because postgres.pgTimeseries.enabled=false.
      @Qualifier("pgTimeseriesStoreRegistry")
          ObjectProvider<PgTimeseriesStoreRegistry> registryProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      QueryFilterRewriteChain queryFilterRewriteChain,
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    if (!postgresSqlSetupProperties.getPgTimeseries().isEnabled()) {
      throw new IllegalStateException(
          "timeseriesAspectService.implementation=postgres requires"
              + " postgres.pgTimeseries.enabled=true (DATAHUB_PGTIMESERIES_ENABLED=true)");
    }
    PgTimeseriesStoreRegistry registry = registryProvider.getIfAvailable();
    if (registry == null) {
      throw new IllegalStateException(
          "timeseriesAspectService.implementation=postgres but pgTimeseriesStoreRegistry is not"
              + " available; set postgres.pgTimeseries.enabled=true with a PostgreSQL"
              + " postgres.pgTimeseries.pool.url (or ebean.url)");
    }

    return new PostgresTimeseriesAspectService(
        registry,
        configurationProvider.getTimeseriesAspectService(),
        queryFilterRewriteChain,
        entityRegistry);
  }
}
