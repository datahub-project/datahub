package com.linkedin.gms.factory.businessattribute;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.BusinessAttributeService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
public class BusinessAttributeServiceFactory {
  @Bean(name = "businessAttributeService")
  @Scope("singleton")
  @Nonnull
  protected BusinessAttributeService businessAttributeService(
      final EntityService<?> entityService) {
    return new BusinessAttributeService(entityService);
  }
}
