package com.linkedin.datahub.graphql;

import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.idl.RuntimeWiring;
import java.util.Collection;
import java.util.List;

/**
 * An interface that allows the Core GMS GraphQL Engine to be extended without requiring code
 * changes in the GmsGraphQLEngine class if new entities, relationships or resolvers need to be
 * introduced. This is useful if you are maintaining a fork of DataHub and don't want to deal with
 * merge conflicts.
 */
public interface GmsGraphQLPlugin {

  /**
   * Initialization method that allows the plugin to instantiate
   *
   * @param args
   */
  void init(GmsGraphQLEngineArgs args);

  /**
   * Return a list of schema files that contain graphql definitions that are served by this plugin
   *
   * @return
   */
  List<String> getSchemaFiles();

  /**
   * Return a list of LoadableTypes that this plugin serves
   *
   * @return
   */
  Collection<? extends LoadableType<?, ?>> getLoadableTypes();

  /**
   * Optional callback that a plugin can implement to configure any Query, Mutation or Type specific
   * resolvers.
   *
   * @param wiringBuilder : the builder being used to configure the runtime wiring
   * @param baseEngine : a reference to the core engine and its graphql types
   */
  default void configureExtraResolvers(
      final RuntimeWiring.Builder wiringBuilder, final GmsGraphQLEngine baseEngine) {}
}
