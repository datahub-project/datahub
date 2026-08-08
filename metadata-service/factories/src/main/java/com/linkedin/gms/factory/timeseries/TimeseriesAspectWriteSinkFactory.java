package com.linkedin.gms.factory.timeseries;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.write.TimeseriesAspectWriteSink;
import com.linkedin.metadata.timeseries.write.postgres.PostgresTimeseriesAspectWriteSink;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Slf4j
@Import(PgTimeseriesConfigOverlay.class)
public class TimeseriesAspectWriteSinkFactory {

  @Bean
  @Nonnull
  public TimeseriesAspectWriteSink timeseriesAspectWriteSink(
      @Qualifier("pgTimeseriesStoreRegistry")
          ObjectProvider<PgTimeseriesStoreRegistry> registryProvider,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Value("${timeseriesAspectService.implementation:elasticsearch}")
          String timeseriesImplementation) {
    if ("postgres".equalsIgnoreCase(timeseriesImplementation.trim())) {
      return TimeseriesAspectWriteSink.NOOP;
    }
    if (!postgresSqlSetupProperties.getPgTimeseries().isEnabled()) {
      return TimeseriesAspectWriteSink.NOOP;
    }
    PgTimeseriesStoreRegistry registry = registryProvider.getIfAvailable();
    if (registry == null) {
      log.warn(
          "postgres.pgTimeseries.enabled but pgTimeseriesStoreRegistry is not available; skipping"
              + " PostgreSQL timeseries dual-write");
      return TimeseriesAspectWriteSink.NOOP;
    }
    return new PostgresTimeseriesAspectWriteSink(
        registry, postgresSqlSetupProperties.getPgTimeseries().isDualWriteFailOnError());
  }
}
