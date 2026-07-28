package com.linkedin.metadata.kafka.config;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.kafka.listener.InstrumentedClientProxy;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the RFC-0 per-hook external-read counter by wrapping the client beans hooks call with
 * {@link InstrumentedClientProxy}. Gated off by default — enable with {@code
 * metadataChangeLog.hook.externalReadsMetric.enabled=true} (env {@code
 * METADATACHANGELOG_HOOK_EXTERNALREADSMETRIC_ENABLED=true}) — so the wrapping stays dark until an
 * operator opts in.
 *
 * <p>Scope/limitations: covers only interface-typed clients ({@link GraphService}, {@link
 * EntitySearchService}, {@link SystemEntityClient}/{@link EntityClient}) since JDK dynamic proxies
 * require an interface. {@code IncidentService} and {@code EventProducer} are class/abstract-class
 * typed and are not wrapped (their fan-out is still visible via the {@code fanout.size} metric).
 * This post-processor runs in the standalone MAE consumer context; embedded-in-GMS topologies would
 * need the same bean registered in the GMS context.
 */
@Configuration
@ConditionalOnProperty(
    name = "metadataChangeLog.hook.externalReadsMetric.enabled",
    havingValue = "true")
public class HookInstrumentationConfiguration {

  @Bean
  static BeanPostProcessor hookClientInstrumentationPostProcessor(
      @Qualifier("systemOperationContext")
          ObjectProvider<OperationContext> systemOperationContextProvider) {

    // Resolved at call time — an ObjectProvider keeps this post-processor free of eager bean
    // dependencies, which BeanPostProcessors must avoid.
    Supplier<MetricUtils> metricUtilsSupplier =
        () -> {
          OperationContext opContext = systemOperationContextProvider.getIfAvailable();
          return opContext == null ? null : opContext.getMetricUtils().orElse(null);
        };

    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName) {
        // Wrap by the most specific interface the bean is injected as. SystemEntityClient extends
        // EntityClient, so it must be checked first to keep both injection points satisfiable.
        if (bean instanceof SystemEntityClient) {
          return InstrumentedClientProxy.wrap(
              SystemEntityClient.class, (SystemEntityClient) bean, metricUtilsSupplier);
        }
        if (bean instanceof EntityClient) {
          return InstrumentedClientProxy.wrap(
              EntityClient.class, (EntityClient) bean, metricUtilsSupplier);
        }
        if (bean instanceof GraphService) {
          return InstrumentedClientProxy.wrap(
              GraphService.class, (GraphService) bean, metricUtilsSupplier);
        }
        if (bean instanceof EntitySearchService) {
          return InstrumentedClientProxy.wrap(
              EntitySearchService.class, (EntitySearchService) bean, metricUtilsSupplier);
        }
        return bean;
      }
    };
  }
}
