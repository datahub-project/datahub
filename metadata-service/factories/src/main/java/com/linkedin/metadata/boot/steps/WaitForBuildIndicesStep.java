package com.linkedin.metadata.boot.steps;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WaitForBuildIndicesStep implements BootstrapStep {

  private final BootstrapDependency _buildIndicesKafkaListener;
  private final ConfigurationProvider _enableWaitForBuildIndices;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute() throws Exception {
    if (!_buildIndicesKafkaListener.waitForBootstrap()) {
      throw new IllegalStateException("Build indices was unsuccessful, stopping bootstrap process.");
    }
  }
}
