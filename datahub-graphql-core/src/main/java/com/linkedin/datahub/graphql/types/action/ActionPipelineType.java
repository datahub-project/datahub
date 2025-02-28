package com.linkedin.datahub.graphql.types.action;

import static com.linkedin.metadata.Constants.DEFAULT_EXECUTOR_ID;

import com.google.common.collect.ImmutableSet;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ActionPipelineType
    implements com.linkedin.datahub.graphql.types.EntityType<ActionPipeline, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.ACTIONS_PIPELINE_INFO_ASPECT_NAME);

  private final EntityClient _entityClient;

  public ActionPipelineType(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public EntityType type() {
    return EntityType.ACTIONS_PIPELINE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<ActionPipeline> objectClass() {
    return ActionPipeline.class;
  }

  @Override
  public List<DataFetcherResult<ActionPipeline>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    if (AuthorizationUtils.canManageActionPipelines(context)) {

      final List<Urn> actionPipelineUrns =
          urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
      try {
        final Map<Urn, EntityResponse> entities =
            _entityClient.batchGetV2(
                context.getOperationContext(),
                Constants.ACTIONS_PIPELINE_ENTITY_NAME,
                new HashSet<>(actionPipelineUrns),
                ASPECTS_TO_FETCH);

        final List<EntityResponse> gmsResults = new ArrayList<>();
        for (Urn urn : actionPipelineUrns) {
          gmsResults.add(entities.getOrDefault(urn, null));
        }
        return gmsResults.stream()
            .map(
                gmsResult ->
                    gmsResult == null
                        ? null
                        : DataFetcherResult.<ActionPipeline>newResult()
                            .data(ActionPipelineType.map(gmsResult))
                            .build())
            .collect(Collectors.toList());
      } catch (Exception e) {
        throw new RuntimeException("Failed to batch load actions pipelines", e);
      }
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  public static ActionPipeline map(final EntityResponse entityResponse) {
    final ActionPipeline result = new ActionPipeline();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ACTIONS_PIPELINE);

    final EnvelopedAspect envelopedActionPipelineInfo =
        aspects.get(Constants.ACTIONS_PIPELINE_INFO_ASPECT_NAME);
    if (envelopedActionPipelineInfo == null) {
      // If we don't get pipeline info back, then it must be missing.
      return null;
    }

    result.setDetails(
        mapActionPipelineDetails(
            new DataHubActionInfo(envelopedActionPipelineInfo.getValue().data())));

    return result;
  }

  private static ActionPipelineDetails mapActionPipelineDetails(
      final DataHubActionInfo actionInfo) {
    final ActionPipelineDetails result = new ActionPipelineDetails();
    result.setName(actionInfo.getName());
    result.setType(actionInfo.getType());
    result.setCategory(actionInfo.getCategory());
    result.setDescription(actionInfo.getDescription());

    ActionConfig config = new ActionConfig();
    config.setDebugMode(false);
    config.setRecipe(actionInfo.getConfig().getRecipe());
    if (actionInfo.hasConfig() && actionInfo.getConfig().hasExecutorId()) {
      config.setExecutorId(actionInfo.getConfig().getExecutorId());
    } else {
      config.setExecutorId(DEFAULT_EXECUTOR_ID);
    }
    result.setConfig(config);
    switch (actionInfo.getState()) {
      case ACTIVE:
        result.setState(ActionPipelineState.ACTIVE);
        break;
      case INACTIVE:
        result.setState(ActionPipelineState.INACTIVE);
        break;
      default:
        result.setState(null);
    }
    return result;
  }
}
