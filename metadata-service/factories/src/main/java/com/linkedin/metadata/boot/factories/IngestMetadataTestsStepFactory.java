package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.boot.steps.IngestMetadataTestsStep;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntityServiceFactory.class})
public class IngestMetadataTestsStepFactory {

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Bean(name = "ingestMetadataTestsStep")
  @Nonnull
  protected IngestMetadataTestsStep createInstance(ConfigurationProvider configurationProvider) {
    return new IngestMetadataTestsStep(
        systemOperationContext, entityService, configurationProvider.getMetadataTests());
  }
}
