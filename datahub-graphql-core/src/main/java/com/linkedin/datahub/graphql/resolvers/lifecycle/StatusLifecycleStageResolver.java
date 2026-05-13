package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.ENTITY_NAME;
import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.INFO_ASPECT;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.datahub.graphql.generated.Status;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves Status.lifecycleStage by fetching the lifecycleStageTypeInfo aspect for the URN stored
 * in the partially-populated LifecycleStageType set by StatusMapper.
 *
 * <p>StatusMapper populates a skeleton LifecycleStageType with only the urn field. This resolver
 * enriches it with the full stage definition (name, description, settings, transition policy).
 */
@Slf4j
@RequiredArgsConstructor
public class StatusLifecycleStageResolver
    implements DataFetcher<CompletableFuture<LifecycleStageType>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<LifecycleStageType> get(DataFetchingEnvironment environment)
      throws Exception {
    final Status status = environment.getSource();
    final LifecycleStageType skeleton = status.getLifecycleStage();

    if (skeleton == null || skeleton.getUrn() == null || skeleton.getUrn().isBlank()) {
      return CompletableFuture.completedFuture(null);
    }

    final QueryContext context = environment.getContext();
    final String stageUrn = skeleton.getUrn();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return resolveStageDetails(stageUrn, context);
          } catch (Exception e) {
            log.warn("Failed to resolve lifecycle stage details for URN {}", stageUrn, e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private LifecycleStageType resolveStageDetails(
      @Nonnull String stageUrn, @Nonnull QueryContext context) throws Exception {
    Urn urn = UrnUtils.getUrn(stageUrn);
    EntityResponse response =
        _entityClient.getV2(
            context.getOperationContext(), ENTITY_NAME, urn, ImmutableSet.of(INFO_ASPECT));

    if (response == null || !response.getAspects().containsKey(INFO_ASPECT)) {
      return null;
    }

    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo(response.getAspects().get(INFO_ASPECT).getValue().data());

    return LifecycleStageTypeMapper.map(stageUrn, info);
  }
}
