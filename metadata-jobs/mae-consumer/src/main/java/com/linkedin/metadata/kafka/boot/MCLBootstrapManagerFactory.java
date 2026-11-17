package com.linkedin.metadata.kafka.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.config.BootstrapConfigurationSupport;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@Conditional(MetadataChangeLogProcessorCondition.class)
public class MCLBootstrapManagerFactory {

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "mclBootstrapManager")
  @Scope("singleton")
  @Nonnull
  protected BootstrapManager createInstance() {
    final int asyncWorkerThreads =
        BootstrapConfigurationSupport.requireAsyncWorkerThreads(_configurationProvider);
    return new BootstrapManager(List.of(), asyncWorkerThreads);
  }
}
