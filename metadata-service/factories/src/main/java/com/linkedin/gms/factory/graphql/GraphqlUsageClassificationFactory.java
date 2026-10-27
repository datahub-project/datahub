package com.linkedin.gms.factory.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GraphqlUsageClassificationFactory {

  @Bean
  @Nonnull
  public UsageOperationsLoader usageOperationsLoader(
      @Qualifier("usageYamlMapper") ObjectMapper usageYamlMapper) {
    return new UsageOperationsLoader(usageYamlMapper);
  }

  @Bean
  @Nonnull
  public GraphqlUsageClassificationRegistry graphqlUsageClassificationRegistry(
      UsageOperationsLoader usageOperationsLoader) {
    return GraphqlUsageClassificationRegistryBuilder.fromManifest(
        usageOperationsLoader.loadBundled());
  }
}
