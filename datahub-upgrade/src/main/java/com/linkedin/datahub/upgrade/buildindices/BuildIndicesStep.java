package com.linkedin.datahub.upgrade.buildindices;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.shared.ElasticSearchIndexed;

import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class BuildIndicesStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> _services;

  @Override
  public String id() {
    return "BuildIndicesStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        for (ElasticSearchIndexed service : _services) {
          service.reindexAll();
        }
      } catch (Exception e) {
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
