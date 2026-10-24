package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
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
 * The run summary is derived from execution requests, so only the system may write it. The
 * side-effect writes are applied asynchronously, which is why the forward-only and existing-source
 * rules are rechecked here against the state at commit time.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class IngestionSourceRunSummaryValidator extends AspectPayloadValidator {

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
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(INGESTION_SOURCE_ENTITY_NAME)
                    .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                    .build()))
        .build();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return operationContext.isSystemAuth()
        ? Stream.empty()
        : mcpItems.stream()
            .map(
                item ->
                    AspectValidationException.forAuth(
                        item,
                        "Only the system can write " + INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME));
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    if (changeMCPs.isEmpty()) {
      return Stream.empty();
    }
    Set<Urn> existingSources =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                operationContext,
                changeMCPs.stream().map(ChangeMCP::getUrn).collect(Collectors.toSet()),
                Set.of(INGESTION_INFO_ASPECT_NAME))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().containsKey(INGESTION_INFO_ASPECT_NAME))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    return changeMCPs.stream()
        .map(item -> staleWrite(item, existingSources))
        .filter(Objects::nonNull);
  }

  @Nullable
  private static AspectValidationException staleWrite(
      @Nonnull ChangeMCP item, @Nonnull Set<Urn> existingSources) {
    if (!existingSources.contains(item.getUrn())) {
      return AspectValidationException.forFilter(
          item, "Ignoring run summary for missing ingestion source " + item.getUrn());
    }
    return IngestionSourceRunSummaryUtils.shouldReplace(
            item.getPreviousAspect(DataHubIngestionSourceRunSummary.class),
            item.getAspect(DataHubIngestionSourceRunSummary.class))
        ? null
        : AspectValidationException.forFilter(
            item, "Ignoring run summary that does not advance " + item.getUrn());
  }
}
