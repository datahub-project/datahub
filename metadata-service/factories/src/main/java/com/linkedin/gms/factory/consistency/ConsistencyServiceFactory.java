package com.linkedin.gms.factory.consistency;

import com.linkedin.gms.factory.systemmetadata.SystemMetadataScrollClientFactory;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyFixRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.config.ConsistencyChecksConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollClient;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Factory for creating the generic {@link ConsistencyService} and related beans.
 *
 * <p>The service is backend-agnostic: it relies on a {@link SystemMetadataScrollClient}
 * (Elasticsearch or PostgreSQL) rather than a specific store.
 */
@Configuration
@Import(SystemMetadataScrollClientFactory.class)
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.aspect.consistency.check",
      "com.linkedin.metadata.aspect.consistency.check.assertion",
      "com.linkedin.metadata.aspect.consistency.check.monitor",
      "com.linkedin.metadata.aspect.consistency.fix"
    })
public class ConsistencyServiceFactory {

  @Bean(name = "genericConsistencyCheckRegistry")
  @Nonnull
  public ConsistencyCheckRegistry consistencyCheckRegistry(List<ConsistencyCheck> checks) {
    return new ConsistencyCheckRegistry(checks);
  }

  @Bean(name = "genericConsistencyFixRegistry")
  @Nonnull
  public ConsistencyFixRegistry consistencyFixRegistry(List<ConsistencyFix> fixes) {
    return new ConsistencyFixRegistry(fixes);
  }

  @Bean(name = "consistencyService")
  @Nonnull
  public ConsistencyService consistencyService(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("systemMetadataScrollClient") final SystemMetadataScrollClient scrollClient,
      @Qualifier("graphClient") final GraphClient graphClient,
      @Qualifier("genericConsistencyCheckRegistry") final ConsistencyCheckRegistry checkRegistry,
      @Qualifier("genericConsistencyFixRegistry") final ConsistencyFixRegistry fixRegistry,
      final DataHubAppConfiguration appConfig) {
    Map<String, Map<String, String>> checkConfigs = Map.of();
    if (appConfig != null && appConfig.getConsistencyChecks() != null) {
      ConsistencyChecksConfiguration checksConfig = appConfig.getConsistencyChecks();
      checkConfigs = checksConfig.getChecks();
    }

    return new ConsistencyService(
        entityService, scrollClient, graphClient, checkRegistry, fixRegistry, checkConfigs);
  }
}
