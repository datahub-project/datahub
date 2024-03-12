package com.linkedin.datahub.graphql.types.dataplatform;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.dataplatform.mappers.DataPlatformMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataPlatformType implements EntityType<DataPlatform, String> {

  private final EntityClient _entityClient;

  public DataPlatformType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public Class<DataPlatform> objectClass() {
    return DataPlatform.class;
  }

  @Override
  public List<DataFetcherResult<DataPlatform>> batchLoad(
      final List<String> urns, final QueryContext context) {

    final List<Urn> dataPlatformUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> dataPlatformMap =
          _entityClient.batchGetV2(
              DATA_PLATFORM_ENTITY_NAME,
              new HashSet<>(dataPlatformUrns),
              null,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : dataPlatformUrns) {
        gmsResults.add(dataPlatformMap.getOrDefault(urn, null));
      }

      return gmsResults.stream()
          .map(
              gmsPlatform ->
                  gmsPlatform == null
                      ? null
                      : DataFetcherResult.<DataPlatform>newResult()
                          .data(DataPlatformMapper.map(gmsPlatform))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Data Platforms", e);
    }
  }

  @Override
  public com.linkedin.datahub.graphql.generated.EntityType type() {
    return com.linkedin.datahub.graphql.generated.EntityType.DATA_PLATFORM;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }
}
