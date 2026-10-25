package com.linkedin.metadata.ingestion.sideeffects;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.ingestion.IngestionSourceRunSummaryUtils;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps an ingestion source's {@code dataHubIngestionSourceRunSummary} pointing at its most recent
 * execution request, so the source list can be sorted and filtered by last run in search.
 *
 * <p>The checks here only avoid publishing pointless writes. The writes are applied asynchronously,
 * so {@code IngestionSourceRunSummaryValidator} enforces them again at commit time.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class IngestionSourceRunSummarySideEffect extends MCPSideEffect {

  // RESTATE is excluded so restore-indices replays of historical runs do not rewrite summaries.
  private static final List<ChangeType> SUPPORTED_CHANGE_TYPES =
      List.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.UPDATE,
          ChangeType.PATCH);
  private static final Set<String> EXECUTION_REQUEST_ASPECTS =
      Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME);
  private static final Set<String> INGESTION_SOURCE_ASPECTS =
      Set.of(INGESTION_INFO_ASPECT_NAME, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME);

  @Nonnull private AspectPluginConfig config;

  @Nonnull
  public static AspectPluginConfig pluginConfig() {
    return AspectPluginConfig.builder()
        .enabled(true)
        .className(IngestionSourceRunSummarySideEffect.class.getName())
        .supportedOperations(SUPPORTED_CHANGE_TYPES.stream().map(ChangeType::toString).toList())
        .supportedEntityAspectNames(
            EXECUTION_REQUEST_ASPECTS.stream()
                .map(
                    aspectName ->
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                            .aspectName(aspectName)
                            .build())
                .toList())
        .build();
  }

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    if (mclItems.isEmpty()) {
      return Stream.empty();
    }
    Map<Urn, Map<String, MCLItem>> batchByExecutionRequest =
        mclItems.stream()
            .collect(
                Collectors.groupingBy(
                    MCLItem::getUrn,
                    LinkedHashMap::new,
                    Collectors.toMap(MCLItem::getAspectName, item -> item, (first, last) -> last)));
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // The execution request is already committed; failing here would fail the executor's write.
    Map<Urn, Candidate> candidateBySource;
    try {
      candidateBySource =
          candidatesBySource(operationContext, batchByExecutionRequest, aspectRetriever);
    } catch (RuntimeException e) {
      log.error(
          "Failed to read execution requests {} for ingestion source run summaries",
          batchByExecutionRequest.keySet(),
          e);
      return Stream.empty();
    }
    try {
      return summaryUpserts(operationContext, candidateBySource, aspectRetriever).stream();
    } catch (RuntimeException e) {
      log.error(
          "Failed to update run summaries for ingestion sources {} from execution requests {}",
          candidateBySource.keySet(),
          batchByExecutionRequest.keySet(),
          e);
      return Stream.empty();
    }
  }

  @Nonnull
  private static Map<Urn, Candidate> candidatesBySource(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Map<Urn, Map<String, MCLItem>> batchByExecutionRequest,
      @Nonnull AspectRetriever aspectRetriever) {
    Map<Urn, Map<String, Aspect>> stored =
        aspectRetriever.getLatestAspectObjects(
            operationContext, batchByExecutionRequest.keySet(), EXECUTION_REQUEST_ASPECTS);
    return batchByExecutionRequest.entrySet().stream()
        .map(
            entry ->
                candidate(
                    entry.getKey(),
                    entry.getValue(),
                    stored.getOrDefault(entry.getKey(), Map.of())))
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                Candidate::sourceUrn,
                candidate -> candidate,
                (current, next) ->
                    IngestionSourceRunSummaryUtils.shouldReplace(current.summary(), next.summary())
                        ? next
                        : current));
  }

  @Nullable
  private static Candidate candidate(
      @Nonnull Urn executionRequestUrn,
      @Nonnull Map<String, MCLItem> batch,
      @Nonnull Map<String, Aspect> stored) {
    ExecutionRequestInput input =
        fromBatchOrStored(
            batch, stored, EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class);
    Urn sourceUrn = IngestionSourceRunSummaryUtils.ingestionSourceUrn(input);
    if (sourceUrn == null) {
      return null;
    }
    ExecutionRequestResult result =
        fromBatchOrStored(
            batch, stored, EXECUTION_REQUEST_RESULT_ASPECT_NAME, ExecutionRequestResult.class);
    return new Candidate(
        sourceUrn,
        IngestionSourceRunSummaryUtils.buildSummary(executionRequestUrn, input, result),
        batch.getOrDefault(
            EXECUTION_REQUEST_INPUT_ASPECT_NAME, batch.get(EXECUTION_REQUEST_RESULT_ASPECT_NAME)));
  }

  @Nonnull
  private static List<MCPItem> summaryUpserts(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Map<Urn, Candidate> candidateBySource,
      @Nonnull AspectRetriever aspectRetriever) {
    if (candidateBySource.isEmpty()) {
      return List.of();
    }
    Map<Urn, Map<String, Aspect>> storedSources =
        aspectRetriever.getLatestAspectObjects(
            operationContext, candidateBySource.keySet(), INGESTION_SOURCE_ASPECTS);
    return candidateBySource.values().stream()
        .filter(
            candidate ->
                isNewerSummary(
                    candidate, storedSources.getOrDefault(candidate.sourceUrn(), Map.of())))
        .map(
            candidate ->
                (MCPItem)
                    ChangeItemImpl.builder()
                        .urn(candidate.sourceUrn())
                        .changeType(ChangeType.UPSERT)
                        .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                        .recordTemplate(candidate.summary())
                        .auditStamp(candidate.trigger().getAuditStamp())
                        .systemMetadata(candidate.trigger().getSystemMetadata())
                        .build(aspectRetriever))
        .toList();
  }

  private static boolean isNewerSummary(
      @Nonnull Candidate candidate, @Nonnull Map<String, Aspect> storedSource) {
    return storedSource.containsKey(INGESTION_INFO_ASPECT_NAME)
        && IngestionSourceRunSummaryUtils.shouldReplace(
            IngestionSourceRunSummaryUtils.toRecord(
                storedSource,
                INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME,
                DataHubIngestionSourceRunSummary.class),
            candidate.summary());
  }

  @Nullable
  private static <T extends RecordTemplate> T fromBatchOrStored(
      @Nonnull Map<String, MCLItem> batch,
      @Nonnull Map<String, Aspect> stored,
      @Nonnull String aspectName,
      @Nonnull Class<T> clazz) {
    MCLItem fromBatch = batch.get(aspectName);
    return fromBatch != null
        ? fromBatch.getAspect(clazz)
        : IngestionSourceRunSummaryUtils.toRecord(stored, aspectName, clazz);
  }

  private record Candidate(
      Urn sourceUrn, DataHubIngestionSourceRunSummary summary, MCLItem trigger) {}
}
