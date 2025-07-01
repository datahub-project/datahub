package com.linkedin.metadata.kafka.boot;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.boot.steps.WaitForSystemUpdateStep;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@Conditional(MetadataChangeProposalProcessorCondition.class)
public class MCPBootstrapManagerFactory {

  @Autowired
  @Qualifier("dataHubUpgradeKafkaListener")
  private BootstrapDependency _dataHubUpgradeKafkaListener;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "mcpBootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance() {
    final WaitForSystemUpdateStep waitForSystemUpdateStep =
        new WaitForSystemUpdateStep(_dataHubUpgradeKafkaListener, _configurationProvider);

    final List<BootstrapStep> finalSteps = ImmutableList.of(waitForSystemUpdateStep);

    return new BootstrapManager(finalSteps);
  }
}
