package com.linkedin.datahub.graphql;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.context.RelationshipTraversalContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nullable;

/** Provided as input to GraphQL resolvers; used to carry information about GQL request context. */
public interface QueryContext {

  /** Returns true if the current actor is authenticated, false otherwise. */
  boolean isAuthenticated();

  /** Returns the {@link Authentication} associated with the current query context. */
  Authentication getAuthentication();

  /** Returns the current authenticated actor, null if there is none. */
  default Actor getActor() {
    return getAuthentication().getActor();
  }

  /** Returns the current authenticated actor, null if there is none. */
  default String getActorUrn() {
    return getActor().toUrnStr();
  }

  /** Returns the authorizer used to authorize specific actions. */
  Authorizer getAuthorizer();

  /**
   * @return Returns the operational context
   */
  OperationContext getOperationContext();

  DataHubAppConfiguration getDataHubAppConfig();

  /**
   * Max depth when traversing parent chains (glossary nodes, domains, containers, documents).
   * Configurable via {@link com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration}.
   */
  int getMaxParentDepth();

  /**
   * Optional request-scoped context for tracking visited URNs during relationship resolution (cycle
   * detection). Present for GraphQL requests; resolvers should short-circuit when a URN was already
   * visited in this request.
   */
  default Optional<RelationshipTraversalContext> getRelationshipTraversalContext() {
    return Optional.empty();
  }

  /**
   * Returns the {@link DataFetchingEnvironment} associated with the current GraphQL request. Used
   * to optimize aspect fetching based on selected fields.
   *
   * @return the DataFetchingEnvironment, or null if not available
   */
  @Nullable
  DataFetchingEnvironment getDataFetchingEnvironment();

  /**
   * Sets the {@link DataFetchingEnvironment} for the current GraphQL request. Typically called by
   * GraphQL resolvers before entity batch loads.
   */
  void setDataFetchingEnvironment(@Nullable DataFetchingEnvironment environment);

  /**
   * Returns the {@link AspectMappingRegistry} for optimizing aspect fetching based on GraphQL field
   * selections.
   *
   * @return the AspectMappingRegistry, or null if not available
   */
  @Nullable
  AspectMappingRegistry getAspectMappingRegistry();

  /**
   * Sets the {@link AspectMappingRegistry} for this context. Typically called during context
   * initialization.
   */
  void setAspectMappingRegistry(@Nullable AspectMappingRegistry aspectMappingRegistry);
}
