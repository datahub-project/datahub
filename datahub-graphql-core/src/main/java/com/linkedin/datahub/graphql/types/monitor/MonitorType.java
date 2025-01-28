package com.linkedin.datahub.graphql.types.monitor;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.IngestionConfig;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.MonitorInfo;
import com.linkedin.datahub.graphql.resolvers.ingest.CachingEntityIngestionSourceFetcher;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.AssertionMonitorsConfiguration;
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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitorType implements com.linkedin.datahub.graphql.types.EntityType<Monitor, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.MONITOR_KEY_ASPECT_NAME, Constants.MONITOR_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;
  private final CachingEntityIngestionSourceFetcher _ingestionSourceFetcher;

  public MonitorType(
      final EntityClient entityClient,
      final AssertionMonitorsConfiguration assertionMonitorsConfiguration) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient cannot be null");
    _ingestionSourceFetcher =
        new CachingEntityIngestionSourceFetcher(entityClient, assertionMonitorsConfiguration);
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
              context.getOperationContext(),
              Constants.MONITOR_ENTITY_NAME,
              new HashSet<>(monitorUrns),
              ASPECTS_TO_FETCH);

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
                          .data(
                              injectMonitorExecutorId(
                                  MonitorMapper.map(context, gmsResult), context))
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

  /**
   * This method attempts to find an executor id associated with the monitor by looking for the
   * ingestion source associated with the monitored entity.
   */
  @Nullable
  private Monitor injectMonitorExecutorId(
      @Nullable final Monitor monitor, @Nonnull final QueryContext context) {
    final Urn entityUrn = UrnUtils.getUrn(monitor.getEntity().getUrn());

    if (monitor == null || monitor.getInfo() == null) {
      return monitor;
    }

    if (monitor.getInfo() != null) {
      // If executor id is already present, return the monitor as is.
      final MonitorInfo info = monitor.getInfo();
      if (info.getExecutorId() != null) {
        return monitor;
      }
    }

    try {
      // Now, resolve the ingestion source for the entity and attempt to find the executor id based
      // on that.
      final IngestionSource source =
          _ingestionSourceFetcher.getIngestionSourceForEntity(entityUrn, context);
      if (source != null) {
        IngestionConfig config = source.getConfig();
        if (config != null) {
          String executorId = config.getExecutorId();
          if (executorId != null) {
            monitor.getInfo().setExecutorId(executorId);
          }
        }
      }
      return monitor;
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve ingestion source for entity " + entityUrn, e);
    }
  }
}
