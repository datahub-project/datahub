package com.linkedin.metadata.ingestion.sideeffects;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps an ingestion source's {@code dataHubIngestionSourceRunSummary} aspect pointing at its most
 * recent execution request, so the source list can be sorted and filtered by last run in search.
 *
 * <p>Runs post-commit on execution request input and result writes. The summary only moves forward:
 * a write for an older execution request (e.g. a late status update for a superseded run) never
 * replaces the summary of a newer one. Summaries are only written for sources that still have an
 * info aspect, so runs for deleted sources do not recreate them.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class IngestionSourceRunSummarySideEffect extends MCPSideEffect {

  private static final Set<String> EXECUTION_REQUEST_ASPECTS =
      Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME);

  @Nonnull private AspectPluginConfig config;

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
    Map<Urn, Map<String, MCLItem>> batchByExecutionRequest = new LinkedHashMap<>();
    for (MCLItem item : mclItems) {
      if (EXECUTION_REQUEST_ASPECTS.contains(item.getAspectName())
          && !ChangeType.DELETE.equals(item.getChangeType())) {
        batchByExecutionRequest
            .computeIfAbsent(item.getUrn(), urn -> new HashMap<>())
            .put(item.getAspectName(), item);
      }
    }
    if (batchByExecutionRequest.isEmpty()) {
      return Stream.empty();
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Map<Urn, Map<String, Aspect>> storedExecutionRequests =
        aspectRetriever.getLatestAspectObjects(
            operationContext, batchByExecutionRequest.keySet(), EXECUTION_REQUEST_ASPECTS);

    Map<Urn, Candidate> candidateBySource = new HashMap<>();
    for (Map.Entry<Urn, Map<String, MCLItem>> entry : batchByExecutionRequest.entrySet()) {
      Urn executionRequestUrn = entry.getKey();
      Map<String, MCLItem> batch = entry.getValue();
      Map<String, Aspect> stored =
          storedExecutionRequests.getOrDefault(executionRequestUrn, Map.of());
      ExecutionRequestInput input =
          fromBatchOrStored(
              batch, stored, EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class);
      Urn sourceUrn = ingestionSourceUrn(input);
      if (sourceUrn == null) {
        continue;
      }
      ExecutionRequestResult result =
          fromBatchOrStored(
              batch, stored, EXECUTION_REQUEST_RESULT_ASPECT_NAME, ExecutionRequestResult.class);
      Candidate candidate =
          new Candidate(
              buildSummary(executionRequestUrn, input, result), batch.values().iterator().next());
      candidateBySource.merge(
          sourceUrn,
          candidate,
          (current, next) -> shouldReplace(current.summary(), next.summary()) ? next : current);
    }
    if (candidateBySource.isEmpty()) {
      return Stream.empty();
    }

    Map<Urn, Map<String, Aspect>> storedSources =
        aspectRetriever.getLatestAspectObjects(
            operationContext,
            candidateBySource.keySet(),
            Set.of(INGESTION_INFO_ASPECT_NAME, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME));

    return candidateBySource.entrySet().stream()
        .filter(
            entry -> {
              Map<String, Aspect> stored = storedSources.getOrDefault(entry.getKey(), Map.of());
              if (!stored.containsKey(INGESTION_INFO_ASPECT_NAME)) {
                log.debug("Skipping run summary for missing ingestion source {}", entry.getKey());
                return false;
              }
              Aspect existing = stored.get(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME);
              return shouldReplace(
                  existing != null ? new DataHubIngestionSourceRunSummary(existing.data()) : null,
                  entry.getValue().summary());
            })
        .map(
            entry ->
                ChangeItemImpl.builder()
                    .urn(entry.getKey())
                    .changeType(ChangeType.UPSERT)
                    .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                    .recordTemplate(entry.getValue().summary())
                    .auditStamp(entry.getValue().trigger().getAuditStamp())
                    .systemMetadata(entry.getValue().trigger().getSystemMetadata())
                    .build(aspectRetriever));
  }

  @Nullable
  public static Urn ingestionSourceUrn(@Nullable ExecutionRequestInput input) {
    if (input == null || !input.hasSource() || !input.getSource().hasIngestionSource()) {
      return null;
    }
    return input.getSource().getIngestionSource();
  }

  @Nonnull
  public static DataHubIngestionSourceRunSummary buildSummary(
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result) {
    DataHubIngestionSourceRunSummary summary =
        new DataHubIngestionSourceRunSummary()
            .setLastExecutionRequest(executionRequestUrn)
            .setLastRunRequestedAtMillis(input.getRequestedAt());
    if (result != null) {
      summary.setLastRunStatus(result.getStatus());
    }
    return summary;
  }

  /**
   * Whether {@code candidate} should overwrite {@code existing}. An update for the same execution
   * request replaces it when anything changed (e.g. its status); otherwise only a strictly newer
   * request does, so equal timestamps keep the summary stable.
   */
  public static boolean shouldReplace(
      @Nullable DataHubIngestionSourceRunSummary existing,
      @Nonnull DataHubIngestionSourceRunSummary candidate) {
    if (existing == null) {
      return true;
    }
    if (Objects.equals(existing.getLastExecutionRequest(), candidate.getLastExecutionRequest())) {
      return !existing.equals(candidate);
    }
    return candidate.getLastRunRequestedAtMillis() > existing.getLastRunRequestedAtMillis();
  }

  @Nullable
  private static <T extends RecordTemplate> T fromBatchOrStored(
      @Nonnull Map<String, MCLItem> batch,
      @Nonnull Map<String, Aspect> stored,
      @Nonnull String aspectName,
      @Nonnull Class<T> clazz) {
    MCLItem fromBatch = batch.get(aspectName);
    if (fromBatch != null) {
      return fromBatch.getAspect(clazz);
    }
    Aspect fromStore = stored.get(aspectName);
    return fromStore != null ? RecordUtils.toRecordTemplate(clazz, fromStore.data()) : null;
  }

  private record Candidate(DataHubIngestionSourceRunSummary summary, MCLItem trigger) {}
}
