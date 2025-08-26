package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class MigrateDataProcessInstanceEdges implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public MigrateDataProcessInstanceEdges(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      List<String> inputPlatforms,
      List<String> outputPlatforms,
      List<String> parentPlatforms,
      Integer batchSize) {
    if (enabled) {
      _steps =
          List.of(
              new MigrateDataProcessInstanceEdgesStep(
                  opContext,
                  entityService,
                  searchService,
                  reprocessEnabled,
                  inputPlatforms,
                  outputPlatforms,
                  parentPlatforms,
                  batchSize));
    } else {
      _steps = List.of();
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
