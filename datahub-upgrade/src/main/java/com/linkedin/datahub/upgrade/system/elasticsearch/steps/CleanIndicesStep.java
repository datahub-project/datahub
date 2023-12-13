package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;

@Slf4j
public class CleanIndicesStep implements UpgradeStep {
  private final RestHighLevelClient searchClient;
  private final ElasticSearchConfiguration esConfig;
  private final List<ElasticSearchIndexed> indexedServices;

  public CleanIndicesStep(
      final RestHighLevelClient searchClient,
      final ElasticSearchConfiguration esConfig,
      final List<ElasticSearchIndexed> indexedServices) {
    this.searchClient = searchClient;
    this.esConfig = esConfig;
    this.indexedServices = indexedServices;
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
        IndexUtils.getAllReindexConfigs(indexedServices)
            .forEach(
                reindexConfig -> ESIndexBuilder.cleanIndex(searchClient, esConfig, reindexConfig));
      } catch (Exception e) {
        log.error("CleanUpIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
