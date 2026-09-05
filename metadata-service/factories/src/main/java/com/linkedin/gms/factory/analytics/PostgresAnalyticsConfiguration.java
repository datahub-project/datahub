package com.linkedin.gms.factory.analytics;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionService;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.compaction.PostgresAnalyticsCompactionService;
import com.linkedin.metadata.analytics.postgres.flush.PostgresAnalyticsEntityCountSink;
import com.linkedin.metadata.analytics.postgres.flush.PostgresAnalyticsUsageFlushSink;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsSink;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnBean(PgAnalyticsStoreRegistry.class)
public class PostgresAnalyticsConfiguration {

  @Bean
  AnalyticsCompactionService analyticsCompactionService(PgAnalyticsStoreRegistry registry) {
    return new PostgresAnalyticsCompactionService(registry);
  }

  @Bean
  @ConditionalOnProperty(
      name = "postgres.pgAnalytics.sinks.apiUsageFlushEnabled",
      havingValue = "true")
  UsageFlushSink postgresAnalyticsUsageFlushSink(PgAnalyticsStoreRegistry registry) {
    return new PostgresAnalyticsUsageFlushSink(registry);
  }

  @Bean
  @ConditionalOnProperty(
      name = "postgres.pgAnalytics.sinks.entityCountEnabled",
      havingValue = "true")
  EntityCountMetricsSink postgresAnalyticsEntityCountSink(PgAnalyticsStoreRegistry registry) {
    return new PostgresAnalyticsEntityCountSink(registry);
  }
}
