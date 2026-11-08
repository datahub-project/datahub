package com.linkedin.gms.factory.context;

import static com.linkedin.metadata.Constants.ENTITY_TYPE_INFO_ASPECT_NAME;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
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
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.EntityTypeUtils;
import com.linkedin.metadata.utils.NextVersionEntityTypes;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
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
import java.util.function.Predicate;
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
      @Nonnull final SearchClusterRegistry searchClusterRegistry,
      @Nonnull final ConfigurationProvider configurationProvider,
      @Qualifier("systemEntityClient") @Nonnull final SystemEntityClient systemEntityClient,
      @Qualifier("mappingsBuilder") @Nonnull final MappingsBuilder mappingsBuilder,
      @Nonnull final SystemTelemetryContext systemTelemetryContext,
      @Autowired(required = false) @Qualifier("groupService") @Nullable
          final GroupService groupService,
      @Autowired(required = false) @Nullable PrimaryStorageResolver primaryStorageResolver,
      @Qualifier("entityGraphCache") @Lazy @Nonnull final EntityGraphCache entityGraphCache,
      @Autowired(required = false) @Nullable final QueryTimeCanonicalizer queryTimeCanonicalizer,
      @Autowired(required = false) @Nullable
          final SearchTemplateCoordinator searchTemplateCoordinator) {

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
            configurationProvider.getElasticSearch().getSearch(),
            searchClusterRegistry,
            searchTemplateCoordinator);

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

    // Attach before the retrievers capture the instance below: OperationContext is immutable, so a
    // later withX() would leave them holding a context without the canonicalizer.
    if (queryTimeCanonicalizer != null) {
      systemOperationContext =
          systemOperationContext.withQueryTimeCanonicalizer(queryTimeCanonicalizer);
    }

    entityClientAspectRetriever.setSystemOperationContext(systemOperationContext);
    entityServiceAspectRetriever.setSystemOperationContext(systemOperationContext);
    systemGraphRetriever.setSystemOperationContext(systemOperationContext);
    searchServiceSearchRetriever.setSystemOperationContext(systemOperationContext);

    configureUnknownDataHandling(configurationProvider, systemOperationContext);
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
      @Nonnull final SearchClusterRegistry searchClusterRegistry,
      @Nonnull final ConfigurationProvider configurationProvider,
      @Nonnull final SystemTelemetryContext systemTelemetryContext,
      @Qualifier("mappingsBuilder") @Nonnull final MappingsBuilder mappingsBuilder,
      @Autowired(required = false) @Qualifier("groupService") @Nullable
          final GroupService groupService,
      @Autowired(required = false) @Nullable PrimaryStorageResolver primaryStorageResolver,
      @Qualifier("entityGraphCache") @Lazy @Nonnull final EntityGraphCache entityGraphCache,
      @Autowired(required = false) @Nullable final QueryTimeCanonicalizer queryTimeCanonicalizer,
      @Autowired(required = false) @Nullable
          final SearchTemplateCoordinator searchTemplateCoordinator) {

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
            configurationProvider.getElasticSearch().getSearch(),
            searchClusterRegistry,
            searchTemplateCoordinator);

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

    // Attach before the retrievers capture the instance below: OperationContext is immutable, so a
    // later withX() would leave them holding a context without the canonicalizer.
    if (queryTimeCanonicalizer != null) {
      systemOperationContext =
          systemOperationContext.withQueryTimeCanonicalizer(queryTimeCanonicalizer);
    }

    entityClientAspectRetriever.setSystemOperationContext(systemOperationContext);
    systemGraphRetriever.setSystemOperationContext(systemOperationContext);
    searchServiceSearchRetriever.setSystemOperationContext(systemOperationContext);

    configureUnknownDataHandling(configurationProvider, systemOperationContext);
    return systemOperationContext;
  }

  @Nonnull
  private static SearchContext buildSearchContext(
      @Nonnull BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull MappingsBuilder mappingsBuilder,
      @Nullable SearchConfiguration searchConfiguration,
      @Nonnull SearchClusterRegistry searchClusterRegistry,
      @Nullable SearchTemplateCoordinator searchTemplateCoordinator) {
    return SearchContext.builder()
        .indexConvention(components.getIndexConvention())
        .searchClusterAccess(searchClusterRegistry)
        .searchTemplateRuntime(searchTemplateCoordinator)
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

  /**
   * Chooses, for this process, how data a newer version wrote is treated, from {@code
   * featureFlags.unknownEntityAspectToleranceEnabled} and {@code
   * featureFlags.unknownEnumToleranceEnabled}. Every process that runs the unknown-data paths (GMS,
   * the MAE and MCE consumers, upgrade jobs) builds its system operation context here. When unknown
   * entities are tolerated, an unknown entity type is tolerated only if a newer version registered
   * it as an {@code entityType} row.
   */
  @VisibleForTesting
  static void configureUnknownDataHandling(
      @Nonnull final ConfigurationProvider configurationProvider,
      @Nonnull final OperationContext systemOperationContext) {
    configureUnknownDataHandling(
        configurationProvider,
        entityTypeUrn ->
            systemOperationContext
                    .getAspectRetriever()
                    .getLatestAspectObject(
                        systemOperationContext, entityTypeUrn, ENTITY_TYPE_INFO_ASPECT_NAME)
                != null);
  }

  /**
   * As above, for a process that runs the unknown-data paths without this factory's system
   * operation context (e.g. a pod writing through {@code EntityService} in process). {@code
   * entityTypeRowExists} tells whether a newer version registered an entity type.
   */
  @Nonnull
  public static UnknownDataHandling configureUnknownDataHandling(
      @Nonnull final ConfigurationProvider configurationProvider,
      @Nonnull final Predicate<Urn> entityTypeRowExists) {
    final boolean unknownEntityAspectTolerance =
        configurationProvider.getFeatureFlags().isUnknownEntityAspectToleranceEnabled();
    final boolean unknownEnumTolerance =
        configurationProvider.getFeatureFlags().isUnknownEnumToleranceEnabled();
    final UnknownDataHandling handling =
        unknownEntityAspectTolerance || unknownEnumTolerance
            ? TolerantHandling.of(
                unknownEntityAspectTolerance,
                unknownEnumTolerance,
                new NextVersionEntityTypes(entityTypeRowExists))
            : StrictHandling.INSTANCE;
    UnknownDataHandlings.install(handling);
    log.info(
        "Unknown data tolerance: entity types, aspects and references {}; enum values {}",
        unknownEntityAspectTolerance ? "enabled" : "disabled",
        unknownEnumTolerance ? "enabled" : "disabled");
    return handling;
  }
}
