package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanIndicesStep implements UpgradeStep {
  private final SearchClientShim<?> searchClient;
  private final ElasticSearchConfiguration esConfig;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;

  public CleanIndicesStep(
      final SearchClientShim<?> searchClient,
      final ElasticSearchConfiguration esConfig,
      final List<ElasticSearchIndexed> indexedServices,
      final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    this.searchClient = searchClient;
    this.esConfig = esConfig;
    this.indexedServices = indexedServices;
    this.structuredProperties = structuredProperties;
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
        // Each service enumerates and cleans up their own indices
        IndexUtils.getAllReindexConfigs(indexedServices, structuredProperties)
            .forEach(
                reindexConfig ->
                    ESIndexBuilder.cleanOrphanedIndices(searchClient, esConfig, reindexConfig));
      } catch (Exception e) {
        log.error("CleanUpIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
