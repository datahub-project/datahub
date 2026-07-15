package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanIndicesStep implements UpgradeStep {
  private final SearchClientShim<?> searchClient;
  private final ElasticSearchConfiguration esConfig;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private final EntityService<?> entityService;
  private final String upgradeVersion;
  private final boolean incrementalReindexEnabled;
  private final boolean rollbackDualWriteEnabled;

  public CleanIndicesStep(
      final SearchClientShim<?> searchClient,
      final ElasticSearchConfiguration esConfig,
      final List<ElasticSearchIndexed> indexedServices,
      final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      final EntityService<?> entityService,
      final String upgradeVersion,
      final BuildIndicesConfiguration buildIndicesConfig) {
    this.searchClient = searchClient;
    this.esConfig = esConfig;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
    this.entityService = entityService;
    this.upgradeVersion = upgradeVersion;
    this.incrementalReindexEnabled =
        buildIndicesConfig != null && buildIndicesConfig.isIncrementalReindexEnabled();
    this.rollbackDualWriteEnabled =
        buildIndicesConfig != null && buildIndicesConfig.isRollbackDualWriteEnabled();
  }

  @Override
  public String id() {
    return "CleanUpIndicesStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        Set<String> protectedPhysicalIndices =
            incrementalReindexEnabled ? loadProtectedPhysicalIndices(context) : Set.of();

        if (!protectedPhysicalIndices.isEmpty()) {
          log.info(
              "Excluding {} physical index(es) from orphan cleanup while incremental reindex"
                  + " catch-up is in progress: {}",
              protectedPhysicalIndices.size(),
              protectedPhysicalIndices);
        }

        IndexUtils.getAllReindexConfigs(context.opContext(), indexedServices, structuredProperties)
            .forEach(
                reindexConfig ->
                    ESIndexBuilder.cleanOrphanedIndices(
                        searchClient,
                        context.opContext(),
                        esConfig,
                        reindexConfig,
                        protectedPhysicalIndices));
      } catch (Exception e) {
        log.error("CleanUpIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private Set<String> loadProtectedPhysicalIndices(UpgradeContext context) {
    Urn phase1UpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion);
    Urn catchUpUpgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    Map<String, String> phase1State =
        context
            .upgrade()
            .getUpgradeResult(context.opContext(), phase1UpgradeIdUrn, entityService)
            .map(DataHubUpgradeResult::getResult)
            .orElse(null);
    Map<String, String> catchUpState =
        context
            .upgrade()
            .getUpgradeResult(context.opContext(), catchUpUpgradeIdUrn, entityService)
            .map(DataHubUpgradeResult::getResult)
            .orElse(null);

    return IncrementalReindexState.getProtectedPhysicalIndicesForCleanup(
        phase1State,
        catchUpState,
        context.opContext().getSearchContext().getIndexConvention(),
        rollbackDualWriteEnabled);
  }
}
