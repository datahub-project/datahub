package com.linkedin.gms.factory.context;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.client.EntityClientAspectRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.SearchServiceSearchRetriever;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.services.RestrictedService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SystemOperationContextFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  /**
   * Used inside GMS
   *
   * <p>Entity Client and Aspect Retriever implemented by EntityService
   */
  @Nonnull
  @Bean(name = "systemOperationContext")
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "java", matchIfMissing = true)
  protected OperationContext javaSystemOperationContext(
      @Nonnull @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      @Nonnull final OperationContextConfig operationContextConfig,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final RestrictedService restrictedService,
      @Nonnull final GraphRetriever graphRetriever,
      @Nonnull final SearchService searchService) {

    EntityServiceAspectRetriever entityServiceAspectRetriever =
        EntityServiceAspectRetriever.builder()
            .entityRegistry(entityRegistry)
            .entityService(entityService)
            .build();

    SearchServiceSearchRetriever searchServiceSearchRetriever =
        SearchServiceSearchRetriever.builder().searchService(searchService).build();

    OperationContext systemOperationContext =
        OperationContext.asSystem(
            operationContextConfig,
            systemAuthentication,
            entityServiceAspectRetriever.getEntityRegistry(),
            ServicesRegistryContext.builder().restrictedService(restrictedService).build(),
            components.getIndexConvention(),
            RetrieverContext.builder()
                .aspectRetriever(entityServiceAspectRetriever)
                .graphRetriever(graphRetriever)
                .searchRetriever(searchServiceSearchRetriever)
                .build());

    entityServiceAspectRetriever.setSystemOperationContext(systemOperationContext);
    searchServiceSearchRetriever.setSystemOperationContext(systemOperationContext);

    return systemOperationContext;
  }

  /**
   * Used outside GMS
   *
   * <p>Entity Client and Aspect Retriever implemented by Restli call to GMS Entity Client and
   * Aspect Retriever client-side caching enabled
   */
  @Nonnull
  @Bean(name = "systemOperationContext")
  @ConditionalOnProperty(name = "entityClient.impl", havingValue = "restli")
  protected OperationContext restliSystemOperationContext(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull @Qualifier("systemEntityClient") SystemEntityClient systemEntityClient,
      @Nonnull @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      @Nonnull final OperationContextConfig operationContextConfig,
      @Nonnull final RestrictedService restrictedService,
      @Nonnull final GraphRetriever graphRetriever,
      @Nonnull final SearchService searchService) {

    EntityClientAspectRetriever entityServiceAspectRetriever =
        EntityClientAspectRetriever.builder().entityClient(systemEntityClient).build();

    SearchServiceSearchRetriever searchServiceSearchRetriever =
        SearchServiceSearchRetriever.builder().searchService(searchService).build();

    OperationContext systemOperationContext =
        OperationContext.asSystem(
            operationContextConfig,
            systemAuthentication,
            entityRegistry,
            ServicesRegistryContext.builder().restrictedService(restrictedService).build(),
            components.getIndexConvention(),
            RetrieverContext.builder()
                .aspectRetriever(entityServiceAspectRetriever)
                .graphRetriever(graphRetriever)
                .searchRetriever(searchServiceSearchRetriever)
                .build());

    entityServiceAspectRetriever.setSystemOperationContext(systemOperationContext);
    searchServiceSearchRetriever.setSystemOperationContext(systemOperationContext);

    return systemOperationContext;
  }

  @Bean
  @Nonnull
  protected OperationContextConfig operationContextConfig(
      final ConfigurationProvider configurationProvider) {
    return OperationContextConfig.builder()
        .viewAuthorizationConfiguration(configurationProvider.getAuthorization().getView())
        .build();
  }
}
