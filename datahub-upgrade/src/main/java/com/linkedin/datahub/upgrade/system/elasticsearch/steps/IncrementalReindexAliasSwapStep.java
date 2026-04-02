package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Phase 3 non-blocking upgrade step that swaps aliases from the current backing index to the next
 * index created during Phase 1. For each index in the Phase 1 upgrade state with {@code COMPLETED}
 * status:
 *
 * <ol>
 *   <li>Validates document counts between current and next index match exactly
 *   <li>Atomically swaps the alias via {@link ESIndexBuilder#validateAndSwapAlias}
 *   <li>Updates the Phase 1 upgrade state to {@code ALIAS_SWAPPED} so the MAE consumer's poller
 *       detects the change and stops dual-writing
 * </ol>
 *
 * <p>Indices that have already been swapped (status {@code ALIAS_SWAPPED}) are skipped.
 */
@Slf4j
public class IncrementalReindexAliasSwapStep implements UpgradeStep {

  static final String UPGRADE_ID_PREFIX = "IncrementalReindexAliasSwap";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final String upgradeVersion;
  private final Urn upgradeIdUrn;
  private final Urn phase1UpgradeIdUrn;

  public IncrementalReindexAliasSwapStep(
      OperationContext opContext,
      EntityService<?> entityService,
      List<ElasticSearchIndexed> indexedServices,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      String upgradeVersion) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.upgradeVersion = upgradeVersion;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    this.phase1UpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion);
  }

  @Override
  public String id() {
    return UPGRADE_ID_PREFIX + "_" + upgradeVersion;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        Optional<DataHubUpgradeResult> phase1Result =
            context.upgrade().getUpgradeResult(opContext, phase1UpgradeIdUrn, entityService);

        if (phase1Result.isEmpty() || phase1Result.get().getResult() == null) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        Map<String, String> phase1State = phase1Result.get().getResult();
        Map<String, Map<String, String>> allIndexStates =
            IncrementalReindexState.getAllIndexStates(phase1State);

        if (allIndexStates.isEmpty()) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        Map<String, String> swapState = loadPreviousState(context);
        Map<String, ESIndexBuilder> indexBuilders = buildIndexBuilderMap();

        for (Map.Entry<String, Map<String, String>> entry : allIndexStates.entrySet()) {
          String indexName = entry.getKey();
          Map<String, String> indexState = entry.getValue();

          String statusStr = indexState.get(IncrementalReindexState.STATUS);
          if (statusStr == null) {
            continue;
          }

          IncrementalReindexState.Status status = IncrementalReindexState.Status.valueOf(statusStr);

          if (status == IncrementalReindexState.Status.ALIAS_SWAPPED) {
            continue;
          }
          if (status != IncrementalReindexState.Status.COMPLETED) {
            log.info("Skipping alias swap for index {}: status is {}", indexName, status);
            continue;
          }

          String nextIndexName = indexState.get(IncrementalReindexState.NEXT_INDEX_NAME);
          if (nextIndexName == null || nextIndexName.isEmpty()) {
            log.warn("Index {} has COMPLETED status but no nextIndexName, skipping", indexName);
            continue;
          }

          ESIndexBuilder indexBuilder = indexBuilders.get(indexName);
          if (indexBuilder == null) {
            log.error("No index builder found for index: {}", indexName);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          boolean swapped = indexBuilder.validateAndSwapAlias(indexName, nextIndexName);
          if (!swapped) {
            log.error(
                "Alias swap failed for {} -> {}: doc count mismatch", indexName, nextIndexName);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          // Update Phase 1 state so the MAE consumer's poller detects the swap
          DataHubUpgradeState phaseState = phase1Result.get().getState();
          DataHubUpgradeResultConditionalPersist.mergeAndPersist(
              opContext,
              entityService,
              phase1UpgradeIdUrn,
              IncrementalReindexState.persistAliasSwappedMerge(indexName, phaseState));

          // Also track in this step's own state for checkpoint/resumption
          phase1State = IncrementalReindexState.setAliasSwapped(phase1State, indexName);

          swapState = IncrementalReindexState.setAliasSwapped(swapState, indexName);
          checkpoint(context, swapState, DataHubUpgradeState.IN_PROGRESS);

          log.info("Alias swap completed for index {} -> {}", indexName, nextIndexName);
        }

        checkpoint(context, swapState, DataHubUpgradeState.SUCCEEDED);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("IncrementalReindexAliasSwapStep failed", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private Map<String, ESIndexBuilder> buildIndexBuilderMap() {
    Map<String, ESIndexBuilder> map = new HashMap<>();
    for (ElasticSearchIndexed service : indexedServices) {
      try {
        for (ReindexConfig config : service.buildReindexConfigs(opContext, structuredProperties)) {
          map.put(config.name(), service.getIndexBuilder());
        }
      } catch (Exception e) {
        log.warn("Error building index builder map: {}", e.getMessage());
      }
    }
    return map;
  }

  private void checkpoint(
      UpgradeContext context, Map<String, String> state, DataHubUpgradeState upgradeState) {
    try {
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          opContext,
          entityService,
          upgradeIdUrn,
          DataHubUpgradeResultConditionalPersist.replaceEntireResult(state, upgradeState));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> loadPreviousState(UpgradeContext context) {
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);
    if (prevResult.isPresent() && prevResult.get().getResult() != null) {
      return new HashMap<>(prevResult.get().getResult());
    }
    return new HashMap<>();
  }
}
