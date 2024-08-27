package com.linkedin.datahub.graphql.types.query;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class QueryType
    implements com.linkedin.datahub.graphql.types.EntityType<QueryEntity, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME, QUERY_SUBJECTS_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.QUERY;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<QueryEntity> objectClass() {
    return QueryEntity.class;
  }

  @Override
  public List<DataFetcherResult<QueryEntity>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> viewUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      log.debug("Fetching query entities: {}", viewUrns);
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              QUERY_ENTITY_NAME,
              new HashSet<>(viewUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : viewUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<QueryEntity>newResult()
                          .data(QueryMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Queries", e);
    }
  }
}
