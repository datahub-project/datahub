package com.linkedin.datahub.graphql;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
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
   * Returns the {@link DataFetchingEnvironment} associated with the current GraphQL request. This
   * provides access to the GraphQL query structure, requested fields, and other execution context.
   *
   * @return the DataFetchingEnvironment, or null if not available
   */
  @Nullable
  DataFetchingEnvironment getDataFetchingEnvironment();

  /**
   * Sets the {@link DataFetchingEnvironment} for the current GraphQL request. This is typically
   * called by GraphQL resolvers to provide access to the execution context.
   *
   * @param environment the DataFetchingEnvironment to associate with this context
   */
  void setDataFetchingEnvironment(@Nullable DataFetchingEnvironment environment);

  /**
   * Returns the {@link AspectMappingRegistry} for optimizing aspect fetching based on GraphQL
   * field selections.
   *
   * @return the AspectMappingRegistry, or null if not available
   */
  @Nullable
  AspectMappingRegistry getAspectMappingRegistry();

  /**
   * Sets the {@link AspectMappingRegistry} for this context. This is typically called during
   * context initialization.
   *
   * @param aspectMappingRegistry the AspectMappingRegistry to use
   */
  void setAspectMappingRegistry(@Nullable AspectMappingRegistry aspectMappingRegistry);
}
