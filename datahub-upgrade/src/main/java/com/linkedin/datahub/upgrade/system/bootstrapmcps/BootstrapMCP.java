package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

public class BootstrapMCP implements Upgrade {
  private final List<UpgradeStep> _steps;

  public BootstrapMCP(
      OperationContext opContext,
      @Nullable String bootstrapMCPConfig,
      EntityService<?> entityService,
      boolean isBlocking)
      throws IOException {
    if (bootstrapMCPConfig != null && !bootstrapMCPConfig.isEmpty()) {
      _steps =
          BootstrapMCPUtil.generateSteps(opContext, isBlocking, bootstrapMCPConfig, entityService);
    } else {
      _steps = ImmutableList.of();
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
