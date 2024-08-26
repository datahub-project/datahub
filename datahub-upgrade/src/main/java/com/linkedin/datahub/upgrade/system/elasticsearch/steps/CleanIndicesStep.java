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
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;

@Slf4j
public class CleanIndicesStep implements UpgradeStep {
  private final RestHighLevelClient searchClient;
  private final ElasticSearchConfiguration esConfig;
  private final List<ElasticSearchIndexed> indexedServices;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;

  public CleanIndicesStep(
      final RestHighLevelClient searchClient,
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
        IndexUtils.getAllReindexConfigs(indexedServices, structuredProperties)
            .forEach(
                reindexConfig -> ESIndexBuilder.cleanIndex(searchClient, esConfig, reindexConfig));
      } catch (Exception e) {
        log.error("CleanUpIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
