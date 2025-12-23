package com.linkedin.gms.factory.consistency;

import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyFixRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.config.ConsistencyChecksConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Factory for creating the generic {@link ConsistencyService} and related beans.
 *
 * <p>This factory creates:
 *
 * <ul>
 *   <li>{@link ConsistencyCheckRegistry} - registry of all consistency checks
 *   <li>{@link ConsistencyFixRegistry} - registry of all consistency fixes
 *   <li>{@link ConsistencyService} - the main service for checking and fixing consistency issues
 * </ul>
 *
 * <p>The service is used by:
 *
 * <ul>
 *   <li>OpenAPI controllers for ad-hoc consistency checking and fixing
 *   <li>System upgrade jobs for batch consistency maintenance
 * </ul>
 */
@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.aspect.consistency.check",
      "com.linkedin.metadata.aspect.consistency.check.assertion",
      "com.linkedin.metadata.aspect.consistency.check.monitor",
      "com.linkedin.metadata.aspect.consistency.fix"
    })
public class ConsistencyServiceFactory {

  /**
   * Create the consistency check registry with all registered checks.
   *
   * @param checks list of all ConsistencyCheck beans (auto-wired from component scan)
   * @return the registry
   */
  @Bean(name = "genericConsistencyCheckRegistry")
  @Nonnull
  public ConsistencyCheckRegistry consistencyCheckRegistry(List<ConsistencyCheck> checks) {
    return new ConsistencyCheckRegistry(checks);
  }

  /**
   * Create the consistency fix registry with all registered fixes.
   *
   * @param fixes list of all ConsistencyFix beans (auto-wired from component scan)
   * @return the registry
   */
  @Bean(name = "genericConsistencyFixRegistry")
  @Nonnull
  public ConsistencyFixRegistry consistencyFixRegistry(List<ConsistencyFix> fixes) {
    return new ConsistencyFixRegistry(fixes);
  }

  /**
   * Create the generic consistency service.
   *
   * @param entityService entity service for fetching entity data from SQL
   * @param esSystemMetadataDAO system metadata DAO for querying the system metadata index
   * @param graphClient graph client
   * @param checkRegistry the check registry
   * @param fixRegistry the fix registry
   * @param appConfig application configuration containing consistency check configs
   * @return the service
   */
  @Bean(name = "consistencyService")
  @Nonnull
  public ConsistencyService consistencyService(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("esSystemMetadataDAO") final ESSystemMetadataDAO esSystemMetadataDAO,
      @Qualifier("graphClient") final GraphClient graphClient,
      @Qualifier("genericConsistencyCheckRegistry") final ConsistencyCheckRegistry checkRegistry,
      @Qualifier("genericConsistencyFixRegistry") final ConsistencyFixRegistry fixRegistry,
      final DataHubAppConfiguration appConfig) {
    // Extract check configurations from app config
    Map<String, Map<String, String>> checkConfigs = Map.of();
    if (appConfig != null && appConfig.getConsistencyChecks() != null) {
      ConsistencyChecksConfiguration checksConfig = appConfig.getConsistencyChecks();
      checkConfigs = checksConfig.getChecks();
    }

    return new ConsistencyService(
        entityService, esSystemMetadataDAO, graphClient, checkRegistry, fixRegistry, checkConfigs);
  }
}
