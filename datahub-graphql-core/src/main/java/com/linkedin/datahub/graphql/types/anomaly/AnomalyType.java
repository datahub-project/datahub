package com.linkedin.datahub.graphql.types.anomaly;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Anomaly;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class AnomalyType implements com.linkedin.datahub.graphql.types.EntityType<Anomaly, String> {

  static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  public AnomalyType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.ANOMALY;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Anomaly> objectClass() {
    return Anomaly.class;
  }

  @Override
  public List<DataFetcherResult<Anomaly>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> anomalyUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.ANOMALY_ENTITY_NAME,
              new HashSet<>(anomalyUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : anomalyUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Anomaly>newResult()
                          .data(AnomalyMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Anomalies", e);
    }
  }
}
