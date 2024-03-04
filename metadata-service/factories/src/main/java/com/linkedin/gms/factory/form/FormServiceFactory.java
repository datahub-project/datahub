package com.linkedin.gms.factory.form;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class FormServiceFactory {
  @Bean(name = "formService")
  @Scope("singleton")
  @Nonnull
  protected FormService getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      final SystemEntityClient entityClient)
      throws Exception {
    return new FormService(systemOpContext, entityClient);
  }
}
