package com.linkedin.gms.factory.lifecycle;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.LifecycleStageTypeService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class LifecycleStageTypeServiceFactory {

  @Bean(name = "lifecycleStageTypeService")
  @Scope("singleton")
  @Nonnull
  protected LifecycleStageTypeService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext) {
    LifecycleStageTypeService service = new LifecycleStageTypeService(systemEntityClient);
    service.setSystemOperationContext(systemOperationContext);
    // Wire into the search filter path so hidden lifecycle stages are excluded from default search.
    ESUtils.setLifecycleStageTypeService(service);
    return service;
  }
}
