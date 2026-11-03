package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Rejects a run event for an ingestion source that does not exist. A timeseries write also upserts
 * the entity's key aspect, so an event for a deleted source would otherwise recreate it as an empty
 * entity that the source list cannot render.
 */
@Setter
@Getter
@Accessors(chain = true)
public class IngestionSourceRunEventValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Nonnull
  public static AspectPluginConfig pluginConfig() {
    return AspectPluginConfig.builder()
        .enabled(true)
        .className(IngestionSourceRunEventValidator.class.getName())
        .supportedOperations(List.of(ChangeType.UPSERT.toString()))
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(INGESTION_SOURCE_ENTITY_NAME)
                    .aspectName(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME)
                    .build()))
        .build();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    if (mcpItems.isEmpty()) {
      return Stream.empty();
    }
    Set<Urn> sourceUrns = mcpItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> stored =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                operationContext, sourceUrns, Set.of(INGESTION_INFO_ASPECT_NAME));
    return mcpItems.stream()
        .filter(
            item ->
                !stored
                    .getOrDefault(item.getUrn(), Map.of())
                    .containsKey(INGESTION_INFO_ASPECT_NAME))
        .map(
            item ->
                AspectValidationException.forItem(
                    item, "Ingestion source " + item.getUrn() + " does not exist"));
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
