package com.linkedin.datahub.graphql.types.entitytype;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.EntityTypeEntity;
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

@RequiredArgsConstructor
public class EntityTypeType
    implements com.linkedin.datahub.graphql.types.EntityType<EntityTypeEntity, String> {

  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(ENTITY_TYPE_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.ENTITY_TYPE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<EntityTypeEntity> objectClass() {
    return EntityTypeEntity.class;
  }

  @Override
  public List<DataFetcherResult<EntityTypeEntity>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> entityTypeUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              ENTITY_TYPE_ENTITY_NAME,
              new HashSet<>(entityTypeUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : entityTypeUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<EntityTypeEntity>newResult()
                          .data(EntityTypeEntityMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load entity type entities", e);
    }
  }
}
