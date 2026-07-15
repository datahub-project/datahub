package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.TimeseriesAspectServiceImplementation;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({
  ElasticSearchTimeseriesAspectServiceFactory.class,
  TimeseriesAspectWriteSinkFactory.class,
  PostgresTimeseriesAspectServiceConfiguration.class
})
@RequiredArgsConstructor
public class TimeseriesAspectServiceFactory {

  private final ObjectProvider<ElasticSearchTimeseriesAspectService>
      elasticSearchTimeseriesAspectServiceProvider;

  private final ConfigurationProvider configurationProvider;

  private final ObjectProvider<PostgresTimeseriesAspectService>
      postgresTimeseriesAspectServiceProvider;

  @Bean(name = "timeseriesAspectService")
  @Primary
  @Nonnull
  protected TimeseriesAspectService timeseriesAspectService() {
    TimeseriesAspectServiceImplementation impl =
        configurationProvider.getTimeseriesAspectService().getImplementation();
    if (impl == TimeseriesAspectServiceImplementation.postgres) {
      PostgresTimeseriesAspectService pg = postgresTimeseriesAspectServiceProvider.getIfAvailable();
      if (pg == null) {
        throw new IllegalStateException(
            "timeseriesAspectService.implementation=postgres requires Postgres timeseries wiring "
                + "(postgres.pgTimeseries.enabled, PostgreSQL ebean.url, and SqlSetup).");
      }
      return pg;
    }
    return elasticSearchTimeseriesAspectServiceProvider.getObject();
  }
}
