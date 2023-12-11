package com.linkedin.datahub.graphql.plugins.action;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.datahub.graphql.GmsGraphQLPlugin;
import com.linkedin.datahub.graphql.resolvers.action.execution.CreateActionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.action.execution.CreateActionPipelineResolver;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.ActionPipelineConfiguration;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ActionGraphQLPlugin implements GmsGraphQLPlugin {

  private static final String ACTION_ENTITY_TYPE = "DataHubAction";
  private static final String ACTION_SCHEMA_FILE = "action.graphql";
  private EntityClient entityClient;
  private IntegrationsService integrationsService;
  private ActionPipelineConfiguration actionConfiguration;

  private boolean initialized;

  public ActionGraphQLPlugin() {
    this.initialized = false;
  }

  public void init(GmsGraphQLEngineArgs args) {
    this.actionConfiguration = args.getActionPipelineConfiguration();
    this.entityClient = args.getEntityClient();
    this.integrationsService = args.getIntegrationsService();
    this.initialized = true;
  }
  @Override
  public List<String> getSchemaFiles() {
    return List.of(ACTION_SCHEMA_FILE);
  }

  public Map<String, DataFetcher> getMutationDataFetchers() {
    return Map.of(
        "createActionExecutionRequest", new CreateActionExecutionRequestResolver(this.entityClient,
        this.integrationsService,
        this.actionConfiguration),
        "createActionPipeline", new CreateActionPipelineResolver(this.entityClient)
    );
  }

  @Override
  public Collection<? extends LoadableType<?, ?>> getLoadableTypes() {
    return null;
  }

  @Override
  public Collection<? extends EntityType<?, ?>> getEntityTypes() {
    return Collections.EMPTY_LIST;
  }

  public Map<String, DataFetcher> getQueryDataFetchers() {
    return Collections.EMPTY_MAP;
  }


  @Override
  public void configureExtraResolvers(final RuntimeWiring.Builder wiringBuilder, final GmsGraphQLEngine baseEngine) {
    assert this.initialized;
    // Query
    wiringBuilder.type("Query", typeWiring -> typeWiring
        .dataFetchers(getQueryDataFetchers())
    );
    // Mutations
    wiringBuilder.type("Mutation", typeWiring -> typeWiring
        .dataFetchers(getMutationDataFetchers()));
  }
}
