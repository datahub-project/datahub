package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.AspectUtils;
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
 * Transition rules on the destination stage are enforced by LifecycleStageTransitionHook before the
 * aspect is persisted.
 */
@Slf4j
@RequiredArgsConstructor
public class SetLifecycleStageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final String lifecycleStageUrn = environment.getArgument("lifecycleStageUrn");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return setLifecycleStage(urnStr, lifecycleStageUrn, context);
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
    Urn entityUrn = UrnUtils.getUrn(urnStr);
    String entityType = entityUrn.getEntityType();

    // Load the current Status aspect, or start fresh
    Status status = new Status();
    Aspect existing =
        _entityClient.getLatestAspectObject(
            context.getOperationContext(), entityUrn, STATUS_ASPECT_NAME, false);
    if (existing != null) {
      status = new Status(existing.data());
    }

    // Apply the change: set or clear lifecycleStage
    if (lifecycleStageUrn != null && !lifecycleStageUrn.isBlank()) {
      status.setLifecycleStage(UrnUtils.getUrn(lifecycleStageUrn));
    } else {
      status.removeLifecycleStage();
    }

    // Record who made this transition and when
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(context.getActorUrn()));
    status.setLifecycleLastUpdated(auditStamp);

    _entityClient.ingestProposal(
        context.getOperationContext(),
        AspectUtils.buildMetadataChangeProposal(entityUrn, STATUS_ASPECT_NAME, status),
        false);

    log.info(
        "Set lifecycle stage {} on entity {} (type: {})", lifecycleStageUrn, urnStr, entityType);
    return true;
  }
}
