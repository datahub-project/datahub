package com.linkedin.gms.factory.context;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.client.EntityClientAspectRetriever;
import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SystemGraphRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.SearchServiceSearchRetriever;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.EntityTypeUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.PrimaryStorageContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.context.ValidationContext;
import io.datahubproject.metadata.context.usage.instrumentation.SessionContextEnricher;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
@Slf4j
public class SystemOperationContextFactory {

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
      @Nonnull final GraphService graphService,
      @Nonnull final SearchService searchService,
      @Qualifier("baseElasticSearchComponents")
          BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      @Nonnull final ConfigurationProvider configurationProvider,
      @Qualifier("systemEntityClient") @Nonnull final SystemEntityClient systemEntityClient,
      @Qualifier("mappingsBuilder") @Nonnull final MappingsBuilder mappingsBuilder,
      @Nonnull final SystemTelemetryContext systemTelemetryContext,
      @Autowired(required = false) @Qualifier("groupService") @Nullable
          final GroupService groupService,
      @Autowired(required = false) @Nullable PrimaryStorageResolver primaryStorageResolver,
      @Qualifier("entityGraphCache") @Lazy @Nonnull final EntityGraphCache entityGraphCache) {

    EntityServiceAspectRetriever entityServiceAspectRetriever =
        EntityServiceAspectRetriever.builder()
            .entityRegistry(entityRegistry)
            .entityService(entityService)
            .build();

    EntityClientAspectRetriever entityClientAspectRetriever =
        EntityClientAspectRetriever.builder().entityClient(systemEntityClient).build();

    SystemGraphRetriever systemGraphRetriever =
        SystemGraphRetriever.builder().graphService(graphService).build();

    SearchServiceSearchRetriever searchServiceSearchRetriever =
        SearchServiceSearchRetriever.builder().searchService(searchService).build();

    SearchContext searchContext =
        buildSearchContext(
            components,
            entityRegistry,
            mappingsBuilder,
            configurationProvider.getElasticSearch().getSearch());

    OperationContext systemOperationContext =
        OperationContext.asSystem(
            operationContextConfig,
            systemAuthentication,
            entityServiceAspectRetriever.getEntityRegistry(),
            ServicesRegistryContext.builder()
                .restrictedService(restrictedService)
                .actorGroupMembershipService(groupService)
                .build(),
            searchContext,
            RetrieverContext.builder()
                .aspectRetriever(entityServiceAspectRetriever)
                .cachingAspectRetriever(entityClientAspectRetriever)
                .graphRetriever(systemGraphRetriever)
                .searchRetriever(searchServiceSearchRetriever)
                .entityGraphCache(entityGraphCache)
                .build(),
            ValidationContext.builder()
                .alternateValidation(
                    configurationProvider.getFeatureFlags().isAlternateMCPValidation())
                .build(),
            systemTelemetryContext,
            primaryStorageContext(primaryStorageResolver),
            configurationProvider.getAuthentication().isEnforceExistenceEnabled());

    entityClientAspectRetriever.setSystemOperationContext(systemOperationContext);
    entityServiceAspectRetriever.setSystemOperationContext(systemOperationContext);
    systemGraphRetriever.setSystemOperationContext(systemOperationContext);
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
      @Nonnull final GraphService graphService,
      @Nonnull final SearchService searchService,
      @Qualifier("baseElasticSearchComponents")
          BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      @Nonnull final ConfigurationProvider configurationProvider,
      @Nonnull final SystemTelemetryContext systemTelemetryContext,
      @Qualifier("mappingsBuilder") @Nonnull final MappingsBuilder mappingsBuilder,
      @Autowired(required = false) @Qualifier("groupService") @Nullable
          final GroupService groupService,
      @Autowired(required = false) @Nullable PrimaryStorageResolver primaryStorageResolver,
      @Qualifier("entityGraphCache") @Lazy @Nonnull final EntityGraphCache entityGraphCache) {

    EntityClientAspectRetriever entityClientAspectRetriever =
        EntityClientAspectRetriever.builder().entityClient(systemEntityClient).build();

    SystemGraphRetriever systemGraphRetriever =
        SystemGraphRetriever.builder().graphService(graphService).build();

    SearchServiceSearchRetriever searchServiceSearchRetriever =
        SearchServiceSearchRetriever.builder().searchService(searchService).build();

    SearchContext searchContext =
        buildSearchContext(
            components,
            entityRegistry,
            mappingsBuilder,
            configurationProvider.getElasticSearch().getSearch());

    OperationContext systemOperationContext =
        OperationContext.asSystem(
            operationContextConfig,
            systemAuthentication,
            entityRegistry,
            ServicesRegistryContext.builder()
                .restrictedService(restrictedService)
                .actorGroupMembershipService(groupService)
                .build(),
            searchContext,
            RetrieverContext.builder()
                .cachingAspectRetriever(entityClientAspectRetriever)
                .graphRetriever(systemGraphRetriever)
                .searchRetriever(searchServiceSearchRetriever)
                .entityGraphCache(entityGraphCache)
                .build(),
            ValidationContext.builder()
                .alternateValidation(
                    configurationProvider.getFeatureFlags().isAlternateMCPValidation())
                .build(),
            systemTelemetryContext,
            primaryStorageContext(primaryStorageResolver),
            configurationProvider.getAuthentication().isEnforceExistenceEnabled());

    entityClientAspectRetriever.setSystemOperationContext(systemOperationContext);
    systemGraphRetriever.setSystemOperationContext(systemOperationContext);
    searchServiceSearchRetriever.setSystemOperationContext(systemOperationContext);

    return systemOperationContext;
  }

  @Nonnull
  private static SearchContext buildSearchContext(
      @Nonnull BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull MappingsBuilder mappingsBuilder,
      @Nullable SearchConfiguration searchConfiguration) {
    return SearchContext.builder()
        .indexConvention(components.getIndexConvention())
        .searchableFieldTypes(ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
        .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
        .defaultSearchEntityNames(
            resolveEntityTypeList(
                "defaultEntityTypes",
                searchConfiguration != null ? searchConfiguration.getDefaultEntityTypes() : null,
                entityRegistry))
        .defaultAutocompleteEntityNames(
            resolveEntityTypeList(
                "autocompleteEntityTypes",
                searchConfiguration != null
                    ? searchConfiguration.getAutocompleteEntityTypes()
                    : null,
                entityRegistry))
        .defaultBrowseEntityNames(
            resolveEntityTypeList(
                "browseEntityTypes",
                searchConfiguration != null ? searchConfiguration.getBrowseEntityTypes() : null,
                entityRegistry))
        .prioritizedSourceEntityTypes(
            resolveEntityTypeList(
                "prioritizedSourceEntityTypes",
                searchConfiguration != null
                    ? searchConfiguration.getPrioritizedSourceEntityTypes()
                    : null,
                entityRegistry))
        .prioritizedDatahubEntityTypes(
            resolveEntityTypeList(
                "prioritizedDatahubEntityTypes",
                searchConfiguration != null
                    ? searchConfiguration.getPrioritizedDatahubEntityTypes()
                    : null,
                entityRegistry))
        .build();
  }

  @Nonnull
  private static List<String> resolveEntityTypeList(
      @Nonnull String listName,
      @Nullable EntityTypeListConfig config,
      @Nonnull EntityRegistry entityRegistry) {
    List<String> resolved = EntityTypeUtils.resolve(config, entityRegistry);
    if (resolved.isEmpty()) {
      log.warn(
          "Resolved elasticsearch.search.{} to empty; GraphQL will use no entity types for that "
              + "path (not all indices). Check value/add/remove and SEARCH_*_ENTITY_TYPES.",
          listName);
    }
    return resolved;
  }

  @Nonnull
  private static PrimaryStorageContext primaryStorageContext(
      @Nullable PrimaryStorageResolver primaryStorageResolver) {
    if (primaryStorageResolver == null) {
      return PrimaryStorageContext.EMPTY;
    }
    return PrimaryStorageResolver.buildDefaultPrimaryStorageContext(
        primaryStorageResolver.getRegistry());
  }

  @Bean
  @Nonnull
  protected OperationContextConfig operationContextConfig(
      final ConfigurationProvider configurationProvider,
      @Nonnull final EntityRegistry entityRegistry,
      @Autowired(required = false) SessionContextEnricher sessionContextEnricher) {
    return OperationContextConfig.builder()
        .viewAuthorizationConfiguration(
            resolveViewAuthorizationConfiguration(
                configurationProvider.getAuthorization().getView(), entityRegistry))
        .sessionContextEnricher(sessionContextEnricher)
        .build();
  }

  @Nonnull
  private static com.datahub.authorization.config.ViewAuthorizationConfiguration
      resolveViewAuthorizationConfiguration(
          @Nullable
              com.datahub.authorization.config.ViewAuthorizationConfiguration viewConfiguration,
          @Nonnull EntityRegistry entityRegistry) {
    if (viewConfiguration == null) {
      viewConfiguration =
          com.datahub.authorization.config.ViewAuthorizationConfiguration.builder().build();
    }
    final Set<String> effective =
        EntityTypeUtils.resolve(viewConfiguration.getUnrestrictedEntityTypes(), entityRegistry);
    return viewConfiguration.toBuilder().effectiveUnrestrictedEntityTypes(effective).build();
  }
}
