package com.linkedin.datahub.graphql.plugins;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.datahub.graphql.GmsGraphQLPlugin;
import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchResolver;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Semantic Search Plugin
 *
 * <p>This plugin adds semantic search capabilities to DataHub. It registers the GraphQL schema and
 * resolvers for semantic search functionality.
 *
 * <p>The plugin is conditionally active - if SemanticSearchService is not available (null), the
 * plugin will not register any resolvers.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>semanticSearch - Search within a single entity type using semantic similarity
 *   <li>semanticSearchAcrossEntities - Search across multiple entity types using semantic
 *       similarity
 * </ul>
 *
 * <p>Requirements:
 *
 * <ul>
 *   <li>OpenSearch 2.17+ with k-NN plugin enabled
 *   <li>SemanticSearchService configured with embedding provider
 *   <li>Semantic indices created for target entities
 * </ul>
 */
@Slf4j
public class SemanticSearchPlugin implements GmsGraphQLPlugin {

  private static final String SEMANTIC_SEARCH_SCHEMA_FILE = "semantic-search.graphql";

  private SemanticSearchService semanticSearchService;
  private ViewService viewService;
  private FormService formService;
  private EntityClient entityClient;

  @Override
  public void init(GmsGraphQLEngineArgs args) {
    this.semanticSearchService = args.getSemanticSearchService();
    this.viewService = args.getViewService();
    this.formService = args.getFormService();
    this.entityClient = args.getEntityClient();

    if (this.semanticSearchService == null) {
      log.info(
          "SemanticSearchService is not configured. Semantic search GraphQL endpoints will not be available.");
    } else {
      log.info("SemanticSearchPlugin initialized successfully with SemanticSearchService");
    }
  }

  @Override
  public List<String> getSchemaFiles() {
    // Always load the schema so the field is defined for clients. When the
    // service is unavailable, the resolver throws SemanticSearchDisabledException
    // which surfaces as a clear runtime GraphQL error instead of FieldUndefined
    // — giving clients a stable contract for feature detection.
    return List.of(SEMANTIC_SEARCH_SCHEMA_FILE);
  }

  @Override
  public Collection<? extends LoadableType<?, ?>> getLoadableTypes() {
    // Semantic search doesn't introduce new loadable types
    return Collections.emptyList();
  }

  @Override
  public Collection<? extends EntityType<?, ?>> getEntityTypes() {
    // Semantic search doesn't introduce new entity types
    return Collections.emptyList();
  }

  @Override
  public void configureExtraResolvers(RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    if (semanticSearchService == null) {
      // Register stub resolvers that throw SemanticSearchDisabledException so
      // clients see a stable runtime error ("Semantic search is disabled in
      // this environment") instead of a schema-level FieldUndefined error.
      // This lets clients (e.g. the integrations service) reliably detect
      // unavailability and fall back to keyword search.
      log.info(
          "SemanticSearchService not available — registering disabled stubs for semantic search resolvers");
      final DataFetcher<Object> disabledStub =
          env -> {
            throw new SemanticSearchDisabledException();
          };
      builder.type(
          "Query",
          typeWiring ->
              typeWiring
                  .dataFetcher("semanticSearch", disabledStub)
                  .dataFetcher("semanticSearchAcrossEntities", disabledStub));
      return;
    }

    builder.type(
        "Query",
        typeWiring ->
            typeWiring
                .dataFetcher("semanticSearch", new SemanticSearchResolver(semanticSearchService))
                .dataFetcher(
                    "semanticSearchAcrossEntities",
                    new SemanticSearchAcrossEntitiesResolver(
                        semanticSearchService, viewService, formService, entityClient)));

    log.info(
        "Registered semantic search GraphQL resolvers: semanticSearch, semanticSearchAcrossEntities");
  }
}
