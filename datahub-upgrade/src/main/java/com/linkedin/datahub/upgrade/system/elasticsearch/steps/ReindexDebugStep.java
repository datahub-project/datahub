package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexDebugStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> services;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  // ours
  private ElasticSearchIndexed service;
  private ReindexConfig config;

  @SneakyThrows
  public ReindexDebugStep(
      List<ElasticSearchIndexed> services,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    this.services = services;
    this.structuredProperties = structuredProperties;
    // pick ours
    for (ElasticSearchIndexed es : services) {
      if (es instanceof ElasticSearchService) {
        this.service = es;
        break;
      }
    }
    List<ReindexConfig> configs = service.buildReindexConfigs(structuredProperties);
    // datahubpolicyindex_v2 has some docs upon starting quickdebug...
    String TARGETINDEX = "datahubpolicyindex_v2";
    for (ReindexConfig cfg : configs) {
      String cfgname = cfg.name();
      if (TARGETINDEX.equalsIgnoreCase(cfgname)) {
        config = cfg;
        config.requiresReindex = true;
        config.requiresApplyMappings = true;
        config.requiresApplySettings = true;
        break;
      }
    }
  }

  @Override
  public String id() {
    return "ReindexDebugStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        try {
          service.getIndexBuilder().buildIndex(config);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } catch (Exception e) {
        log.error("BuildIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
