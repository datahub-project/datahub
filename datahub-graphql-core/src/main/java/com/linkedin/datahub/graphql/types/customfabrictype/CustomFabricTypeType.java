package com.linkedin.datahub.graphql.types.customfabrictype;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CustomFabricTypeType
    implements com.linkedin.datahub.graphql.types.EntityType<CustomFabricType, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME,
          CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME);

  private final EntityClient entityClient;

  @Override
  public EntityType type() {
    return EntityType.CUSTOM_FABRIC_TYPE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<CustomFabricType> objectClass() {
    return CustomFabricType.class;
  }

  @Override
  public List<DataFetcherResult<CustomFabricType>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> typeUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {
      Set<String> aspects =
          AspectUtils.getOptimizedAspects(
              context, name(), ASPECTS_TO_FETCH, CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> entities =
          entityClient.batchGetV2(
              context.getOperationContext(),
              CUSTOM_FABRIC_TYPE_ENTITY_NAME,
              new HashSet<>(typeUrns),
              aspects);
      return typeUrns.stream()
          .map(entities::get)
          .map(
              response ->
                  response == null
                      ? null
                      : DataFetcherResult.<CustomFabricType>newResult()
                          .data(CustomFabricTypeMapper.map(context, response))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load custom fabric types", e);
    }
  }
}
