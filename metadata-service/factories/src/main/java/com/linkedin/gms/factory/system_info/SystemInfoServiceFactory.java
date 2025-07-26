package com.linkedin.gms.factory.system_info;

import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.collectors.PropertiesCollector;
import com.linkedin.metadata.system_info.collectors.SpringComponentsCollector;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

@Configuration
public class SystemInfoServiceFactory {

  @Bean(name = "propertiesCollector")
  @Scope("singleton")
  @Nonnull
  protected PropertiesCollector createPropertiesCollector(final Environment springEnvironment) {
    return new PropertiesCollector(springEnvironment);
  }

  @Bean(name = "springComponentsCollector")
  @Scope("singleton")
  @Nonnull
  protected SpringComponentsCollector createSpringComponentsCollector(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      final GitVersion gitVersion,
      final PropertiesCollector propertiesCollector) {
    return new SpringComponentsCollector(systemOperationContext, gitVersion, propertiesCollector);
  }

  @Bean(name = "systemInfoService")
  @Scope("singleton")
  @Nonnull
  protected SystemInfoService createSystemInfoService(
      final SpringComponentsCollector springComponentsCollector,
      final PropertiesCollector propertiesCollector) {
    return new SystemInfoService(springComponentsCollector, propertiesCollector);
  }
}
