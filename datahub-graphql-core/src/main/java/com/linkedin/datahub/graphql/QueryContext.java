package com.linkedin.datahub.graphql;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.context.RelationshipTraversalContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;
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
   * Returns the {@link AspectMappingRegistry} for optimizing aspect fetching based on GraphQL field
   * selections.
   *
   * @return the AspectMappingRegistry, or null if not available
   */
  @Nullable
  default AspectMappingRegistry getAspectMappingRegistry() {
    return null;
  }

  /**
   * Sets the {@link AspectMappingRegistry} for this context. Typically called during context
   * initialization.
   */
  default void setAspectMappingRegistry(@Nullable AspectMappingRegistry aspectMappingRegistry) {}

  /**
   * Merges a per-field {@link AspectLoadContext} into the request-scoped union for {@code
   * entityTypeName}. Resolvers call this before DataLoader {@code load}.
   *
   * <p>This resolver-side merge is required even with context-aware DataLoader cache keys: when two
   * loads share the same key and the same {@link AspectLoadContext} signature, DataLoader collapses
   * them onto one pending future and later key contexts never reach dispatch. Enqueue-time merge
   * ensures aliased siblings still widen the request-scoped aspect set used by {@code batchLoad}.
   */
  default void mergeAspectLoadContext(
      @Nonnull String entityTypeName, @Nonnull AspectLoadContext loadContext) {}

  /**
   * Returns the request-scoped unioned {@link AspectLoadContext} for {@code entityTypeName}, or
   * null if no resolver contributed a selection for this type.
   */
  @Nullable
  default AspectLoadContext getAspectLoadContext(@Nonnull String entityTypeName) {
    return null;
  }
}
