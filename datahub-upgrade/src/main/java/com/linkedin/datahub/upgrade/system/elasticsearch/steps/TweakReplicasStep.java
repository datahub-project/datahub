package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TweakReplicasStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> services;
  private final Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;

  @Override
  public String id() {
    return "TweakReplicasStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        for (ElasticSearchIndexed service : services) {
          service.tweakReplicasAll(structuredProperties);
        }
      } catch (Exception e) {
        log.error("TweakReplicasStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
