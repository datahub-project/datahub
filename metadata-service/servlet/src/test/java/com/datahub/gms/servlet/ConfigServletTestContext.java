package com.datahub.gms.servlet;

import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.plugins"
    })
public class ConfigServletTestContext {

  @Bean("systemOperationContext")
  @Primary
  public OperationContext systemOperationContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Bean
  @Primary
  @Qualifier("entityService")
  public EntityService<?> entityService() {
    return Mockito.mock(EntityService.class);
  }
}
