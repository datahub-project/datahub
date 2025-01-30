package com.linkedin.metadata.trace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.systemmetadata.TraceService;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.metadata.context.TraceIdGenerator;
import io.datahubproject.metadata.exception.TraceException;
import io.datahubproject.openapi.v1.models.TraceStatus;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Builder
@Slf4j
public class TraceServiceImpl implements TraceService {
  private final EntityRegistry entityRegistry;
  private final SystemMetadataService systemMetadataService;
  private final EntityService<?> entityService;
  private final MCPTraceReader mcpTraceReader;
  private final MCPFailedTraceReader mcpFailedTraceReader;
  private final MCLTraceReader mclVersionedTraceReader;
  private final MCLTraceReader mclTimeseriesTraceReader;

  public TraceServiceImpl(
      EntityRegistry entityRegistry,
      SystemMetadataService systemMetadataService,
      EntityService<?> entityService,
      MCPTraceReader mcpTraceReader,
      MCPFailedTraceReader mcpFailedTraceReader,
      MCLTraceReader mclVersionedTraceReader,
      MCLTraceReader mclTimeseriesTraceReader) {
    this.entityRegistry = entityRegistry;
    this.systemMetadataService = systemMetadataService;
    this.entityService = entityService;
    this.mcpTraceReader = mcpTraceReader;
    this.mcpFailedTraceReader = mcpFailedTraceReader;
    this.mclVersionedTraceReader = mclVersionedTraceReader;
    this.mclTimeseriesTraceReader = mclTimeseriesTraceReader;
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, TraceStatus>> trace(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      @Nonnull Map<Urn, List<String>> aspectNames,
      boolean onlyIncludeErrors,
      boolean detailed,
      boolean skipCache) {

    long traceTimestampMillis = TraceIdGenerator.getTimestampMillis(traceId);

    // Get primary status for all URNs
    Map<Urn, LinkedHashMap<String, TraceStorageStatus>> primaryStatuses =
        tracePrimaryInParallel(
            opContext, traceId, traceTimestampMillis, aspectNames, detailed, skipCache);

    // Get search status for all URNs using primary results
    Map<Urn, LinkedHashMap<String, TraceStorageStatus>> searchStatuses =
        traceSearchInParallel(
            opContext, traceId, traceTimestampMillis, aspectNames, primaryStatuses, skipCache);

    // Merge and filter results for each URN
    Map<Urn, Map<String, TraceStatus>> mergedResults =
        aspectNames.keySet().stream()
            .collect(
                Collectors.toMap(
                    urn -> urn,
                    urn ->
                        mergeStatus(
                            primaryStatuses.getOrDefault(urn, new LinkedHashMap<>()),
                            searchStatuses.getOrDefault(urn, new LinkedHashMap<>()),
                            onlyIncludeErrors)));

    // Remove URNs with empty aspect maps (when filtering for errors)
    return mergedResults.entrySet().stream()
        .filter(entry -> !entry.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<Urn, LinkedHashMap<String, TraceStorageStatus>> tracePrimaryInParallel(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      long traceTimestampMillis,
      @Nonnull Map<Urn, List<String>> aspectNames,
      boolean detailed,
      boolean skipCache) {

    // Group aspects by whether they are timeseries
    Map<Urn, Map<String, TraceStorageStatus>> timeseriesResults = new HashMap<>();
    Map<Urn, Set<String>> nonTimeseriesAspects = new HashMap<>();

    for (Map.Entry<Urn, List<String>> entry : aspectNames.entrySet()) {
      Urn urn = entry.getKey();
      EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());

      Map<String, TraceStorageStatus> timeseriesStatuses = new LinkedHashMap<>();
      Set<String> remainingAspects = new HashSet<>();

      for (String aspectName : entry.getValue()) {
        if (entitySpec.getAspectSpec(aspectName).isTimeseries()) {
          timeseriesStatuses.put(aspectName, TraceStorageStatus.NO_OP);
        } else {
          remainingAspects.add(aspectName);
        }
      }

      if (!timeseriesStatuses.isEmpty()) {
        timeseriesResults.put(urn, timeseriesStatuses);
      }
      if (!remainingAspects.isEmpty()) {
        nonTimeseriesAspects.put(urn, remainingAspects);
      }
    }

    // Process non-timeseries aspects using SQL
    Map<Urn, Map<String, TraceStorageStatus>> sqlResults = new HashMap<>();
    if (!nonTimeseriesAspects.isEmpty()) {
      try {
        Map<Urn, EntityResponse> responses =
            entityService.getEntitiesV2(
                opContext,
                nonTimeseriesAspects.keySet().iterator().next().getEntityType(),
                nonTimeseriesAspects.keySet(),
                nonTimeseriesAspects.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet()),
                false);

        for (Map.Entry<Urn, EntityResponse> entry : responses.entrySet()) {
          Map<String, TraceStorageStatus> aspectStatuses = new LinkedHashMap<>();
          for (Map.Entry<String, EnvelopedAspect> aspectEntry :
              entry.getValue().getAspects().entrySet()) {
            long createdOnMillis = aspectEntry.getValue().getCreated().getTime();
            SystemMetadata systemMetadata = aspectEntry.getValue().getSystemMetadata();
            String systemTraceId = extractTraceId(systemMetadata);
            Optional<Long> aspectLastUpdated = extractLastUpdated(systemMetadata);
            String aspectName = aspectEntry.getKey();

            if (traceId.equals(systemTraceId)) {
              aspectStatuses.put(aspectName, TraceStorageStatus.ok(TraceWriteStatus.ACTIVE_STATE));
            } else if (traceTimestampMillis <= extractTimestamp(systemTraceId, createdOnMillis)) {
              aspectStatuses.put(
                  aspectName, TraceStorageStatus.ok(TraceWriteStatus.HISTORIC_STATE));
            } else if (createdOnMillis < traceTimestampMillis
                && traceTimestampMillis < aspectLastUpdated.orElse(traceTimestampMillis)) {
              aspectStatuses.put(aspectName, TraceStorageStatus.ok(TraceWriteStatus.NO_OP));
            }
          }
          sqlResults.put(entry.getKey(), aspectStatuses);
        }
      } catch (Exception e) {
        log.error("Error getting entities", e);
      }
    }

    // Account for sql results
    Map<Urn, List<String>> remainingAspects = new HashMap<>();
    for (Map.Entry<Urn, Set<String>> entry : nonTimeseriesAspects.entrySet()) {
      Set<String> foundAspects =
          sqlResults.getOrDefault(entry.getKey(), Collections.emptyMap()).keySet();
      Set<String> remaining = new HashSet<>(entry.getValue());
      remaining.removeAll(foundAspects);
      if (!remaining.isEmpty()) {
        remainingAspects.put(entry.getKey(), new ArrayList<>(remaining));
      }
    }

    // Get remaining aspects from Kafka
    Map<Urn, Map<String, TraceStorageStatus>> kafkaResults =
        mcpTraceReader.tracePendingStatuses(
            remainingAspects, traceId, traceTimestampMillis, skipCache);

    // Merge all results
    Map<Urn, LinkedHashMap<String, TraceStorageStatus>> finalResults = new HashMap<>();
    for (Urn urn : aspectNames.keySet()) {
      LinkedHashMap<String, TraceStorageStatus> merged = new LinkedHashMap<>();
      merged.putAll(timeseriesResults.getOrDefault(urn, Collections.emptyMap()));
      merged.putAll(sqlResults.getOrDefault(urn, Collections.emptyMap()));
      merged.putAll(kafkaResults.getOrDefault(urn, Collections.emptyMap()));
      finalResults.put(urn, merged);
    }

    if (detailed) {
      handleFailedMCP(opContext, finalResults, traceId, traceTimestampMillis);
    }

    return finalResults;
  }

  private Optional<Long> extractLastUpdated(@Nullable SystemMetadata systemMetadata) {
    return Optional.ofNullable(systemMetadata)
        .flatMap(sysMeta -> Optional.ofNullable(sysMeta.getLastObserved()));
  }

  private void handleFailedMCP(
      @Nonnull OperationContext opContext,
      Map<Urn, LinkedHashMap<String, TraceStorageStatus>> finalResults,
      @Nonnull String traceId,
      long traceTimestampMillis) {
    // Create a map of URNs and aspects that need to be checked in the failed topic
    Map<Urn, List<String>> aspectsToCheck = new HashMap<>();

    // Filter for aspects with ERROR, NO_OP, or UNKNOWN status that might be in the failed topic
    for (Map.Entry<Urn, LinkedHashMap<String, TraceStorageStatus>> entry :
        finalResults.entrySet()) {
      Urn urn = entry.getKey();
      EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());

      /*
       * ERROR - to fetch exception
       * NO_OP - to validate there wasn't a failure during an expected NO_OP
       * UNKNOWN - ambiguous case resolution
       */
      List<String> aspectsToVerify =
          entry.getValue().entrySet().stream()
              .filter(aspect -> !entitySpec.getAspectSpec(aspect.getKey()).isTimeseries())
              .filter(
                  aspect ->
                      Set.of(
                              TraceWriteStatus.ERROR,
                              TraceWriteStatus.NO_OP,
                              TraceWriteStatus.UNKNOWN)
                          .contains(aspect.getValue().getWriteStatus()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toList());

      if (!aspectsToVerify.isEmpty()) {
        aspectsToCheck.put(entry.getKey(), aspectsToVerify);
      }
    }

    // If there are no aspects to check, return early
    if (aspectsToCheck.isEmpty()) {
      return;
    }

    try {
      // Find messages in the failed topic for these URNs and aspects
      Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>
          failedMessages =
              mcpFailedTraceReader.findMessages(aspectsToCheck, traceId, traceTimestampMillis);

      // Update the status for any aspects found in the failed topic
      for (Map.Entry<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>
          entry : failedMessages.entrySet()) {
        Urn urn = entry.getKey();
        LinkedHashMap<String, TraceStorageStatus> urnStatuses = finalResults.get(urn);

        if (urnStatuses != null) {
          for (Map.Entry<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>
              aspectEntry : entry.getValue().entrySet()) {
            String aspectName = aspectEntry.getKey();

            // If we found the message in the failed topic, update its status (possible transition
            // from UNKNOWN)
            TraceStorageStatus.TraceStorageStatusBuilder builder =
                TraceStorageStatus.builder().writeStatus(TraceWriteStatus.ERROR);

            // Populate the exception if possible
            mcpFailedTraceReader
                .read(aspectEntry.getValue().getFirst().value())
                .ifPresent(
                    failedMCP ->
                        builder.writeExceptions(extractTraceExceptions(opContext, failedMCP)));

            urnStatuses.put(aspectName, builder.build());
          }
        }
      }
    } catch (Exception e) {
      log.error("Error processing failed MCP messages", e);
    }
  }

  private Map<Urn, LinkedHashMap<String, TraceStorageStatus>> traceSearchInParallel(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      long traceTimestampMillis,
      @Nonnull Map<Urn, List<String>> aspectNames,
      @Nonnull Map<Urn, LinkedHashMap<String, TraceStorageStatus>> primaryStatuses,
      boolean skipCache) {

    Map<Urn, List<String>> aspectsToResolve = new HashMap<>();
    Map<Urn, LinkedHashMap<String, TraceStorageStatus>> finalResults = new HashMap<>();

    // 1. Consider status of primary storage write
    for (Map.Entry<Urn, List<String>> entry : aspectNames.entrySet()) {
      Urn urn = entry.getKey();
      EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
      LinkedHashMap<String, TraceStorageStatus> finalResponse = new LinkedHashMap<>();
      List<String> remaining = new ArrayList<>();

      Map<String, TraceStorageStatus> primaryStatus =
          primaryStatuses.getOrDefault(urn, new LinkedHashMap<>());

      for (String aspectName : entry.getValue()) {
        TraceWriteStatus status = primaryStatus.get(aspectName).getWriteStatus();
        if (status == TraceWriteStatus.PENDING) {
          finalResponse.put(
              aspectName,
              TraceStorageStatus.ok(TraceWriteStatus.PENDING, "Pending primary storage write."));
        } else if (status == TraceWriteStatus.NO_OP) {
          if (entitySpec.getAspectSpec(aspectName).isTimeseries()) {
            finalResponse.put(
                aspectName, TraceStorageStatus.ok(TraceWriteStatus.TRACE_NOT_IMPLEMENTED));
          } else {
            finalResponse.put(aspectName, TraceStorageStatus.NO_OP);
          }
        } else if (status == TraceWriteStatus.ERROR) {
          finalResponse.put(
              aspectName,
              TraceStorageStatus.fail(TraceWriteStatus.ERROR, "Primary storage write failed."));
        } else if (status == TraceWriteStatus.TRACE_NOT_IMPLEMENTED
            || status == TraceWriteStatus.UNKNOWN) {
          finalResponse.put(
              aspectName,
              TraceStorageStatus.ok(
                  TraceWriteStatus.UNKNOWN, "Primary storage write indeterminate."));
        } else {
          remaining.add(aspectName);
        }
      }

      if (!remaining.isEmpty()) {
        aspectsToResolve.put(urn, remaining);
      }
      if (!finalResponse.isEmpty()) {
        finalResults.put(urn, finalResponse);
      }
    }

    // 2. Check implied search write using system metadata
    if (!aspectsToResolve.isEmpty()) {
      // Get system metadata & group by URN
      Map<Urn, List<AspectRowSummary>> summariesByUrn =
          aspectsToResolve.entrySet().stream()
              .flatMap(
                  entry ->
                      systemMetadataService
                          .findAspectsByUrn(entry.getKey(), entry.getValue(), true)
                          .stream())
              .collect(Collectors.groupingBy(summary -> UrnUtils.getUrn(summary.getUrn())));

      // Process each URN's summaries
      for (Map.Entry<Urn, List<String>> entry : aspectsToResolve.entrySet()) {
        Urn urn = entry.getKey();
        List<String> remaining = new ArrayList<>(entry.getValue());
        LinkedHashMap<String, TraceStorageStatus> response =
            finalResults.computeIfAbsent(urn, k -> new LinkedHashMap<>());

        for (AspectRowSummary summary : summariesByUrn.getOrDefault(urn, Collections.emptyList())) {
          if (traceId.equals(summary.getTelemetryTraceId())) {
            response.put(
                summary.getAspectName(), TraceStorageStatus.ok(TraceWriteStatus.ACTIVE_STATE));
            remaining.remove(summary.getAspectName());
          } else if (summary.hasTimestamp()
              && summary.getTimestamp() > 0
              && traceTimestampMillis <= summary.getTimestamp()) {
            response.put(
                summary.getAspectName(), TraceStorageStatus.ok(TraceWriteStatus.HISTORIC_STATE));
            remaining.remove(summary.getAspectName());
          }
        }

        // update remaining
        aspectsToResolve.put(urn, remaining);
      }

      // Get remaining from Kafka
      Map<Urn, Map<String, TraceStorageStatus>> kafkaResults =
          mcpTraceReader.tracePendingStatuses(
              aspectsToResolve, traceId, traceTimestampMillis, skipCache);

      // Merge Kafka results
      kafkaResults.forEach(
          (urn, statuses) ->
              finalResults.computeIfAbsent(urn, k -> new LinkedHashMap<>()).putAll(statuses));
    }

    return finalResults;
  }

  private static Map<String, TraceStatus> mergeStatus(
      LinkedHashMap<String, TraceStorageStatus> primaryAspectStatus,
      LinkedHashMap<String, TraceStorageStatus> searchAspectStatus,
      boolean onlyIncludeErrors) {

    return primaryAspectStatus.entrySet().stream()
        .map(
            storageEntry -> {
              String aspectName = storageEntry.getKey();
              TraceStorageStatus primaryStatus = storageEntry.getValue();
              TraceStorageStatus searchStatus = searchAspectStatus.get(aspectName);
              TraceStatus traceStatus =
                  TraceStatus.builder()
                      .primaryStorage(primaryStatus)
                      .searchStorage(searchStatus)
                      .success(isSuccess(primaryStatus, searchStatus))
                      .build();

              // Only include this aspect if we're not filtering for errors
              // or if either storage has an ERROR status
              if (!onlyIncludeErrors
                  || TraceWriteStatus.ERROR.equals(primaryStatus.getWriteStatus())
                  || TraceWriteStatus.ERROR.equals(searchStatus.getWriteStatus())) {
                return Map.entry(aspectName, traceStatus);
              }
              return null;
            })
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existing, replacement) -> existing,
                LinkedHashMap::new));
  }

  private static boolean isSuccess(
      TraceStorageStatus primaryStatus, TraceStorageStatus searchStatus) {
    return !TraceWriteStatus.ERROR.equals(primaryStatus.getWriteStatus())
        && !TraceWriteStatus.ERROR.equals(searchStatus.getWriteStatus());
  }

  @Nullable
  private static String extractTraceId(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata != null && systemMetadata.getProperties() != null) {
      return systemMetadata.getProperties().get(TraceContext.TELEMETRY_TRACE_KEY);
    }
    return null;
  }

  private static long extractTimestamp(@Nullable String traceId, long createOnMillis) {
    return Optional.ofNullable(traceId)
        .map(TraceIdGenerator::getTimestampMillis)
        .orElse(createOnMillis);
  }

  private List<TraceException> extractTraceExceptions(
      @Nonnull OperationContext opContext, FailedMetadataChangeProposal fmcp) {
    if (!fmcp.getError().isEmpty()) {
      try {
        if (fmcp.getError().startsWith("[") && fmcp.getError().endsWith("]")) {
          return opContext.getObjectMapper().readValue(fmcp.getError(), new TypeReference<>() {});
        }
      } catch (Exception e) {
        log.warn("Failed to deserialize: {}", fmcp.getError());
      }
      return List.of(new TraceException(fmcp.getError()));
    }
    return List.of(new TraceException("Unable to extract trace exception"));
  }
}
