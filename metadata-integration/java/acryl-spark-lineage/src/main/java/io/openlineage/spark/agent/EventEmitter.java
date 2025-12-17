/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.TransportFactory;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.agent.lifecycle.plan.column.RddLineageEventInterceptor;
import io.openlineage.spark.agent.lifecycle.plan.column.SchemaHistoryTracker;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.DebugConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private Optional<String> overriddenAppName;
  @Getter private String jobNamespace;
  @Getter private Optional<String> parentJobName;
  @Getter private Optional<String> parentJobNamespace;
  @Getter private Optional<UUID> parentRunId;
  @Getter private Optional<String> rootParentJobName;
  @Getter private Optional<String> rootParentJobNamespace;
  @Getter private Optional<UUID> rootParentRunId;
  @Getter private UUID applicationRunId;
  @Getter private String applicationJobName;
  @Getter private Optional<List<String>> customEnvironmentVariables;
  @Getter private RddLineageEventInterceptor rddLineageInterceptor;

  public EventEmitter(SparkOpenLineageConfig config, String applicationJobName)
      throws URISyntaxException {
    this.jobNamespace = config.getNamespace();
    this.parentJobName = Optional.ofNullable(config.getParentJobName());
    this.parentJobNamespace = Optional.ofNullable(config.getParentJobNamespace());
    this.parentRunId = convertToUUID(config.getParentRunId());
    this.rootParentJobName = Optional.ofNullable(config.getRootParentJobName());
    this.rootParentJobNamespace = Optional.ofNullable(config.getRootParentJobNamespace());
    this.rootParentRunId = convertToUUID(config.getRootParentRunId());
    this.overriddenAppName = Optional.ofNullable(config.getOverriddenAppName());
    this.customEnvironmentVariables =
        config.getFacetsConfig() != null
            ? config.getFacetsConfig().getCustomEnvironmentVariables() != null
                ? Optional.of(
                    Arrays.asList(config.getFacetsConfig().getCustomEnvironmentVariables()))
                : Optional.empty()
            : Optional.empty();

    List<String> disabledFacets =
        Stream.of(config.getFacetsConfig().getEffectiveDisabledFacets())
            .collect(Collectors.toList());
    // make sure DebugFacet is not disabled if smart debug is enabled
    // debug facet will be only sent when triggered with smart debug. Facet filtering is done
    // on the Spark side, so we can exclude it here
    Optional.ofNullable(config.getDebugConfig())
        .filter(DebugConfig::isSmartEnabled)
        .ifPresent(e -> disabledFacets.remove("debug"));

    this.client =
        OpenLineageClient.builder()
            .transport(new TransportFactory(config.getTransportConfig()).build())
            .disableFacets(disabledFacets.toArray(new String[0]))
            .build();
    this.applicationJobName = applicationJobName;
    this.applicationRunId = UUIDUtils.generateNewUUID();

    // Initialize RDD lineage interceptor
    this.rddLineageInterceptor = initializeRddLineageInterceptor(config);
  }

  private RddLineageEventInterceptor initializeRddLineageInterceptor(
      SparkOpenLineageConfig config) {
    ColumnLineageConfig columnLineageConfig = config.getColumnLineageConfig();
    if (columnLineageConfig == null) {
      log.debug("Column lineage config is null, RDD lineage correction will be disabled");
      return new RddLineageEventInterceptor(false);
    }

    boolean enabled = columnLineageConfig.isRddLineageCorrectionEnabled();
    if (!enabled) {
      log.info("RDD lineage correction is disabled via configuration");
      return new RddLineageEventInterceptor(false);
    }

    // Create schema tracker with configured settings
    long maxAge = columnLineageConfig.getRddLineageSchemaMaxAge();
    int maxSnapshots = columnLineageConfig.getRddLineageSchemaMaxSnapshots();
    SchemaHistoryTracker schemaTracker = new SchemaHistoryTracker(maxAge, maxSnapshots);

    log.info(
        "RDD lineage correction enabled with maxAge={}ms, maxSnapshots={}", maxAge, maxSnapshots);
    return new RddLineageEventInterceptor(true, schemaTracker);
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      // Apply RDD lineage correction if enabled
      OpenLineage.RunEvent correctedEvent = rddLineageInterceptor.intercept(event);

      this.client.emit(correctedEvent);
      if (log.isDebugEnabled()) {
        log.debug(
            "Emitting lineage completed successfully  with run id: {}: {}",
            correctedEvent.getRun().getRunId(),
            OpenLineageClientUtils.toJson(correctedEvent));
      } else {
        log.info(
            "Emitting lineage completed successfully with run id: {}",
            correctedEvent.getRun().getRunId());
      }
    } catch (OpenLineageClientException exception) {
      log.error("Could not emit lineage w/ exception", exception);
    }
  }

  private static Optional<UUID> convertToUUID(String uuid) {
    try {
      return Optional.ofNullable(uuid).map(UUID::fromString);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  public void close() {
    // Close the OpenLineage client if needed
    // The client doesn't have a close method in older versions, so we just ensure cleanup
  }
}
