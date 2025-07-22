package com.linkedin.gms.factory.template;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.PageTemplateService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PageTemplateServiceFactory {
  @Bean(name = "pageTemplateService")
  @Nonnull
  protected PageTemplateService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new PageTemplateService(entityClient);
  }
}
