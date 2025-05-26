package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.ReindexDebugArgs;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexDebugStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> services;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  @Getter public ReindexDebugArgs args;
  // ours
  private ElasticSearchIndexed service;
  private ReindexConfig config;

  private String getIndex(final Map<String, Optional<String>> parsedArgs) {
    String index = null;
    if (containsKey(parsedArgs, "index")) {
      index = parsedArgs.get("index").get();
    }
    return index != null ? index : "";
  }

  public ReindexDebugArgs createArgs(UpgradeContext context) {
    if (args != null) {
      return args;
    } else {
      ReindexDebugArgs result = new ReindexDebugArgs();
      result.index = getIndex(context.parsedArgs());
      args = result;
      return result;
    }
  }

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
      ReindexDebugArgs args = createArgs(context);
      try {
        setConfig(args.index);
        try {
          service.getIndexBuilder().buildIndex(config);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } catch (Exception e) {
        log.error("ReindexDebugStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private void setConfig(String targetIndex) throws IOException, IllegalAccessException {
    // datahubpolicyindex_v2 has some docs upon starting quickdebug...
    //  String targetIndex = "datahubpolicyindex_v2";
    List<ReindexConfig> configs = service.buildReindexConfigs(structuredProperties);
    for (ReindexConfig cfg : configs) {
      String cfgname = cfg.name();
      if (cfgname.startsWith(targetIndex)) {
        config = cfg;
        config.forceReindex();
        break;
      }
    }
  }
}
