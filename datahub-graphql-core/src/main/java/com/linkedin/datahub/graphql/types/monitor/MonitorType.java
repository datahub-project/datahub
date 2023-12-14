package com.linkedin.datahub.graphql.types.monitor;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class MonitorType implements com.linkedin.datahub.graphql.types.EntityType<Monitor, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  public MonitorType(final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient cannot be null");
  }

  @Override
  public EntityType type() {
    return EntityType.MONITOR;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Monitor> objectClass() {
    return Monitor.class;
  }

  @Override
  public List<DataFetcherResult<Monitor>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> monitorUrns = urns.stream().map(this::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              Constants.MONITOR_ENTITY_NAME,
              new HashSet<>(monitorUrns),
              ASPECTS_TO_FETCH,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : monitorUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Monitor>newResult()
                          .data(MonitorMapper.map(gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Monitors", e);
    }
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}
