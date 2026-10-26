package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the setLifecycleStage mutation: sets or clears the lifecycle stage on any entity.
 *
 * <p>Passing null for lifecycleStageUrn clears the stage (publishes/activates the entity).
 * Transition rules on the destination stage are checked up front and rejected with an error;
 * LifecycleStageTransitionHook still guards every other writer of Status.
 */
@Slf4j
@RequiredArgsConstructor
public class SetLifecycleStageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final String lifecycleStageUrn = environment.getArgument("lifecycleStageUrn");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!LifecycleStageAssignmentUtils.canEditLifecycleStage(
              context, UrnUtils.getUrn(urnStr))) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          try {
            return setLifecycleStage(urnStr, lifecycleStageUrn, context);
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to set lifecycle stage {} on entity {}", lifecycleStageUrn, urnStr, e);
            throw new RuntimeException(
                String.format("Failed to set lifecycle stage on entity %s", urnStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean setLifecycleStage(
      @Nonnull String urnStr, String lifecycleStageUrn, @Nonnull QueryContext context)
      throws Exception {
    final Urn entityUrn = UrnUtils.getUrn(urnStr);
    final String entityType = entityUrn.getEntityType();
    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

    Urn stageUrn = null;
    if (lifecycleStageUrn != null && !lifecycleStageUrn.isBlank()) {
      stageUrn = UrnUtils.getUrn(lifecycleStageUrn);
      LifecycleStageAssignmentUtils.validateUserAssignment(
          context, _entityService, entityUrn, stageUrn);
    }

    // Load the current Status aspect, or start fresh
    Status status =
        (Status)
            EntityUtils.getAspectFromEntity(
                context.getOperationContext(),
                urnStr,
                STATUS_ASPECT_NAME,
                _entityService,
                new Status());

    // Apply the change: set or clear lifecycleStage
    if (stageUrn != null) {
      status.setLifecycleStage(stageUrn);
    } else {
      status.removeLifecycleStage();
    }
    status.setLifecycleLastUpdated(
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn));

    EntityUtils.ingestChangeProposals(
        context.getOperationContext(),
        ImmutableList.of(
            AspectUtils.buildMetadataChangeProposal(entityUrn, STATUS_ASPECT_NAME, status)),
        _entityService,
        actorUrn,
        false);

    log.info(
        "Set lifecycle stage {} on entity {} (type: {})", lifecycleStageUrn, urnStr, entityType);
    return true;
  }
}
