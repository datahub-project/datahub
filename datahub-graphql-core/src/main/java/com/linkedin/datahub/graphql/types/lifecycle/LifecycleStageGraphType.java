package com.linkedin.datahub.graphql.types.lifecycle;

import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.ENTITY_NAME;
import static com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper.INFO_ASPECT;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.datahub.graphql.resolvers.lifecycle.LifecycleStageTypeMapper;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads {@link LifecycleStageType} objects by URN via the standard {@link LoadableType} /
 * DataLoader mechanism, enabling batch-fetching and caching of lifecycle stage type entities.
 */
@Slf4j
@RequiredArgsConstructor
public class LifecycleStageGraphType implements LoadableType<LifecycleStageType, String> {

  private static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(INFO_ASPECT, STATUS_ASPECT_NAME);

  private final EntityClient _entityClient;

  @Override
  public Class<LifecycleStageType> objectClass() {
    return LifecycleStageType.class;
  }

  @Override
  public List<DataFetcherResult<LifecycleStageType>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final Set<Urn> urnSet = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(), ENTITY_NAME, new HashSet<>(urnSet), ASPECTS_TO_FETCH);
      return urns.stream()
          .map(
              urn -> {
                EntityResponse response = entities.get(UrnUtils.getUrn(urn));
                return response == null
                    ? null
                    : DataFetcherResult.<LifecycleStageType>newResult()
                        .data(LifecycleStageTypeMapper.map(context, response))
                        .build();
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load LifecycleStageType entities", e);
    }
  }
}
