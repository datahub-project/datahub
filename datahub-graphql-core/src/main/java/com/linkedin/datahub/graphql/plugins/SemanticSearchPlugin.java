package com.linkedin.datahub.graphql.plugins;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.datahub.graphql.GmsGraphQLPlugin;
import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.semantic.SemanticSearchResolver;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
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
    // Only load the schema if semantic search is available
    if (semanticSearchService != null) {
      return List.of(SEMANTIC_SEARCH_SCHEMA_FILE);
    }
    return Collections.emptyList();
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
    // Only register resolvers if semantic search is available
    if (semanticSearchService == null) {
      log.debug(
          "Skipping semantic search resolver registration - SemanticSearchService not available");
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
