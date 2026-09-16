package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.ENTITY_NAME;
import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.INFO_ASPECT;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.search.SearchEntity;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Returns all registered lifecycle stage types.
 *
 * <p>Lifecycle stages are data-driven: operators define their vocabulary by ingesting
 * lifecycleStageType entities (via bootstrap MCPs or ingestion). This resolver searches for all
 * such entities and returns their names, URNs, and settings so that agents and UIs can discover the
 * available stages before calling setLifecycleStage.
 */
@Slf4j
@RequiredArgsConstructor
public class ListLifecycleStagesResolver
    implements DataFetcher<CompletableFuture<List<LifecycleStageType>>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<List<LifecycleStageType>> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return listStages(context);
          } catch (Exception e) {
            log.error("Failed to list lifecycle stages", e);
            throw new RuntimeException("Failed to list lifecycle stages", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private List<LifecycleStageType> listStages(QueryContext context) throws Exception {
    var searchResult =
        _entityClient.search(context.getOperationContext(), ENTITY_NAME, "", null, null, 0, 1000);

    if (searchResult == null || searchResult.getEntities().isEmpty()) {
      return Collections.emptyList();
    }

    Set<Urn> urns =
        searchResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toSet());

    Map<Urn, EntityResponse> responses =
        _entityClient.batchGetV2(
            context.getOperationContext(), ENTITY_NAME, urns, ImmutableSet.of(INFO_ASPECT));

    List<LifecycleStageType> result = new ArrayList<>();
    for (Map.Entry<Urn, EntityResponse> entry : responses.entrySet()) {
      EntityResponse response = entry.getValue();
      if (!response.getAspects().containsKey(INFO_ASPECT)) {
        continue;
      }

      LifecycleStageTypeInfo info =
          new LifecycleStageTypeInfo(response.getAspects().get(INFO_ASPECT).getValue().data());

      result.add(LifecycleStageTypeMapper.map(entry.getKey().toString(), info));
    }

    return result;
  }
}
