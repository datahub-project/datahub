package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.entity.RetentionServiceFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.boot.steps.IngestRetentionPoliciesStep;
import com.linkedin.metadata.entity.RetentionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@Import({RetentionServiceFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IngestRetentionPoliciesStepFactory {

  @Autowired
  @Qualifier("retentionService")
  private RetentionService _retentionService;

  @Value("${entityService.retention.enabled}")
  private Boolean _enableRetention;

  @Value("${entityService.retention.applyOnBootstrap}")
  private Boolean _applyOnBootstrap;

  @Value("${datahub.plugin.retention.path}")
  private String _pluginRegistryPath;

  @Bean(name = "ingestRetentionPoliciesStep")
  @Scope("singleton")
  @Nonnull
  protected IngestRetentionPoliciesStep createInstance() {
    return new IngestRetentionPoliciesStep(_retentionService, _enableRetention, _applyOnBootstrap, _pluginRegistryPath);
  }
}