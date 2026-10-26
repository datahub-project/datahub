package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.ingestion.IngestionSourceRunSummaryUtils;
import java.util.Collection;
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
 * The run summary is derived, so a write is kept only when it matches the execution request it
 * points at and moves the summary forward. The stored run is checked rather than the writer because
 * side-effect writes are validated with the triggering user's context. The checks run at commit
 * time because side-effect writes land asynchronously, and failures are filtered so a stale write
 * never fails its consumer. A summary that lands after its source was deleted also drops the key
 * aspect the write generates, otherwise it would recreate the source as an empty entity.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class IngestionSourceRunSummaryValidator extends AspectPayloadValidator {

  private static final Set<String> EXECUTION_REQUEST_ASPECTS =
      Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME);
  private static final List<ChangeType> SUPPORTED_CHANGE_TYPES =
      List.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.UPDATE,
          ChangeType.PATCH);

  @Nonnull private AspectPluginConfig config;

  @Nonnull
  public static AspectPluginConfig pluginConfig() {
    return AspectPluginConfig.builder()
        .enabled(true)
        .className(IngestionSourceRunSummaryValidator.class.getName())
        .supportedOperations(SUPPORTED_CHANGE_TYPES.stream().map(ChangeType::toString).toList())
        .supportedEntityAspectNames(
            Stream.of(
                    INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME,
                    INGESTION_SOURCE_KEY_ASPECT_NAME,
                    INGESTION_INFO_ASPECT_NAME)
                .map(
                    aspectName ->
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(INGESTION_SOURCE_ENTITY_NAME)
                            .aspectName(aspectName)
                            .build())
                .toList())
        .build();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    Map<String, List<ChangeMCP>> itemsByAspect =
        changeMCPs.stream().collect(Collectors.groupingBy(ChangeMCP::getAspectName));
    List<ChangeMCP> summaries =
        itemsByAspect.getOrDefault(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, List.of());
    if (summaries.isEmpty()) {
      return Stream.empty();
    }
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Set<Urn> missingSources = missingSources(operationContext, aspectRetriever, itemsByAspect);
    Map<Urn, Map<String, Aspect>> runs =
        aspectRetriever.getLatestAspectObjects(
            operationContext,
            summaries.stream()
                .map(
                    item ->
                        item.getAspect(DataHubIngestionSourceRunSummary.class)
                            .getLastExecutionRequest())
                .collect(Collectors.toSet()),
            EXECUTION_REQUEST_ASPECTS);
    return Stream.concat(
        itemsByAspect.getOrDefault(INGESTION_SOURCE_KEY_ASPECT_NAME, List.of()).stream()
            .filter(item -> missingSources.contains(item.getUrn()))
            .map(
                item ->
                    AspectValidationException.forFilter(
                        item, "Ignoring key for missing ingestion source " + item.getUrn())),
        summaries.stream()
            .map(item -> invalidWrite(item, missingSources, runs))
            .filter(Objects::nonNull));
  }

  @Nonnull
  private static Set<Urn> missingSources(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<String, List<ChangeMCP>> itemsByAspect) {
    Set<Urn> sourcesInBatch =
        itemsByAspect.getOrDefault(INGESTION_INFO_ASPECT_NAME, List.of()).stream()
            .map(ChangeMCP::getUrn)
            .collect(Collectors.toSet());
    Set<Urn> summaryUrns =
        itemsByAspect.get(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME).stream()
            .map(ChangeMCP::getUrn)
            .filter(urn -> !sourcesInBatch.contains(urn))
            .collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> storedSources =
        aspectRetriever.getLatestAspectObjects(
            operationContext, summaryUrns, Set.of(INGESTION_INFO_ASPECT_NAME));
    return summaryUrns.stream()
        .filter(
            urn ->
                !storedSources.getOrDefault(urn, Map.of()).containsKey(INGESTION_INFO_ASPECT_NAME))
        .collect(Collectors.toSet());
  }

  @Nullable
  private static AspectValidationException invalidWrite(
      @Nonnull ChangeMCP item,
      @Nonnull Set<Urn> missingSources,
      @Nonnull Map<Urn, Map<String, Aspect>> runs) {
    if (missingSources.contains(item.getUrn())) {
      return AspectValidationException.forFilter(
          item, "Ignoring run summary for missing ingestion source " + item.getUrn());
    }
    DataHubIngestionSourceRunSummary summary =
        item.getAspect(DataHubIngestionSourceRunSummary.class);
    if (!summary.equals(expectedSummary(item.getUrn(), summary, runs))) {
      return AspectValidationException.forFilter(
          item, "Ignoring run summary that does not match its execution request " + item.getUrn());
    }
    return IngestionSourceRunSummaryUtils.shouldReplace(
            item.getPreviousAspect(DataHubIngestionSourceRunSummary.class),
            item.getAspect(DataHubIngestionSourceRunSummary.class))
        ? null
        : AspectValidationException.forFilter(
            item, "Ignoring run summary that does not advance " + item.getUrn());
  }

  @Nullable
  private static DataHubIngestionSourceRunSummary expectedSummary(
      @Nonnull Urn sourceUrn,
      @Nonnull DataHubIngestionSourceRunSummary summary,
      @Nonnull Map<Urn, Map<String, Aspect>> runs) {
    Map<String, Aspect> run = runs.getOrDefault(summary.getLastExecutionRequest(), Map.of());
    ExecutionRequestInput input =
        IngestionSourceRunSummaryUtils.toRecord(
            run, EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class);
    return sourceUrn.equals(IngestionSourceRunSummaryUtils.ingestionSourceUrn(input))
        ? IngestionSourceRunSummaryUtils.buildSummary(
            summary.getLastExecutionRequest(),
            input,
            IngestionSourceRunSummaryUtils.toRecord(
                run, EXECUTION_REQUEST_RESULT_ASPECT_NAME, ExecutionRequestResult.class))
        : null;
  }
}
