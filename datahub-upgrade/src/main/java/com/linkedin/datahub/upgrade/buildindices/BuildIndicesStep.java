package com.linkedin.datahub.upgrade.buildindices;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class BuildIndicesStep implements UpgradeStep {

  private final GraphService _graphService;
  private final EntitySearchService _entitySearchService;
  private final SystemMetadataService _systemMetadataService;
  private final TimeseriesAspectService _timeseriesAspectService;

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
        _graphService.configure();
        _entitySearchService.configure();
        _systemMetadataService.configure();
        _timeseriesAspectService.configure();
      } catch (Exception e) {
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
