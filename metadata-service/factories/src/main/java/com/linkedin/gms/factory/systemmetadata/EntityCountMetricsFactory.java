package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.EntityCountMetricsConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsPublisher;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsSink;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsSinkComposer;
import com.linkedin.metadata.systemmetadata.metrics.MicrometerEntityCountMetricsSink;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "datahub.metrics.entityCounts.enabled",
    havingValue = "true",
    matchIfMissing = true)
public class EntityCountMetricsFactory {

  private EntityCountMetricsPublisher entityCountMetricsPublisher;

  @Bean
  @Nonnull
  public MicrometerEntityCountMetricsSink micrometerEntityCountMetricsSink(
      MeterRegistry meterRegistry) {
    return new MicrometerEntityCountMetricsSink(meterRegistry);
  }

  @Bean
  @Nonnull
  public EntityCountMetricsSinkComposer entityCountMetricsSinkComposer(
      List<EntityCountMetricsSink> entityCountMetricsSinks) {
    List<EntityCountMetricsSink> delegates =
        entityCountMetricsSinks.stream()
            .filter(sink -> !(sink instanceof EntityCountMetricsSinkComposer))
            .toList();
    return new EntityCountMetricsSinkComposer(delegates);
  }

  @Bean
  @Nonnull
  public EntityCountMetricsPublisher entityCountMetricsPublisher(
      @Qualifier("keyAspectEntityCountService")
          KeyAspectEntityCountService keyAspectEntityCountService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      EntityCountMetricsSinkComposer entityCountMetricsSinkComposer,
      @Autowired(required = false)
          MicrometerEntityCountMetricsSink micrometerEntityCountMetricsSink,
      ConfigurationProvider configurationProvider) {
    EntityCountMetricsConfiguration config = resolveEntityCountMetricsConfig(configurationProvider);
    entityCountMetricsPublisher =
        new EntityCountMetricsPublisher(
            keyAspectEntityCountService,
            systemOperationContext,
            entityCountMetricsSinkComposer,
            micrometerEntityCountMetricsSink,
            config);
    return entityCountMetricsPublisher;
  }

  @PreDestroy
  public void shutdownEntityCountMetricsPublisher() {
    if (entityCountMetricsPublisher != null) {
      log.info("Shutting down entity count metrics publisher");
      entityCountMetricsPublisher.close();
      entityCountMetricsPublisher = null;
    }
  }

  @Nonnull
  static EntityCountMetricsConfiguration resolveEntityCountMetricsConfig(
      @Nonnull ConfigurationProvider configurationProvider) {
    DataHubConfiguration dataHubConfiguration = configurationProvider.getDatahub();
    if (dataHubConfiguration.getMetrics() == null
        || dataHubConfiguration.getMetrics().getEntityCounts() == null) {
      return new EntityCountMetricsConfiguration();
    }
    return dataHubConfiguration.getMetrics().getEntityCounts();
  }
}
