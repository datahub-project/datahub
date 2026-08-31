package com.linkedin.gms.factory.module;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.PageModuleService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PageModuleServiceFactory {
  @Bean(name = "pageModuleService")
  @Nonnull
  protected PageModuleService getInstance(
      @Qualifier("entityClient") @Nonnull final EntityClient entityClient) {
    return new PageModuleService(entityClient);
  }
}
