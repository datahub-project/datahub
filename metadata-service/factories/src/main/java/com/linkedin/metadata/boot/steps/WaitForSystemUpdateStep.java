package com.linkedin.metadata.boot.steps;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WaitForSystemUpdateStep implements BootstrapStep {

  private final BootstrapDependency _dataHubUpgradeKafkaListener;
  private final ConfigurationProvider _enableWaitForSystemUpdate;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext) throws Exception {
    if (!_dataHubUpgradeKafkaListener.waitForBootstrap()) {
      throw new IllegalStateException(
          "Build indices was unsuccessful, stopping bootstrap process.");
    }
  }
}
