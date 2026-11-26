package com.linkedin.metadata.ingestion.hooks;

import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.ingestion.IngestionSourceRunEvents;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Merges a proposed {@code dataHubIngestionSourceRunSummary} with the stored one inside the write
 * transaction (see {@link IngestionSourceRunEvents#merge}). Runs of a source are processed out of
 * order and by more than one writer, so the merge, not the writer, keeps the summary moving
 * forward.
 */
@Setter
@Getter
@Accessors(chain = true)
public class IngestionSourceRunSummaryMutator extends MutationHook {

  @Nonnull private AspectPluginConfig config;

  @Nonnull
  public static AspectPluginConfig pluginConfig() {
    return AspectPluginConfig.builder()
        .enabled(true)
        .className(IngestionSourceRunSummaryMutator.class.getName())
        .supportedOperations(
            Stream.of(
                    ChangeType.CREATE,
                    ChangeType.CREATE_ENTITY,
                    ChangeType.UPSERT,
                    ChangeType.UPDATE,
                    ChangeType.PATCH)
                .map(ChangeType::toString)
                .toList())
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(INGESTION_SOURCE_ENTITY_NAME)
                    .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                    .build()))
        .build();
  }

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPs.stream()
        .map(
            item ->
                Pair.of(
                    item,
                    INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME.equals(item.getAspectName())
                        && IngestionSourceRunEvents.merge(
                            item.getPreviousAspect(DataHubIngestionSourceRunSummary.class),
                            item.getAspect(DataHubIngestionSourceRunSummary.class))));
  }
}
