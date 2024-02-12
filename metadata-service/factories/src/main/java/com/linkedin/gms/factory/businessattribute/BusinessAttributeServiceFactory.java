package com.linkedin.gms.factory.businessattribute;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.BusinessAttributeService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
public class BusinessAttributeServiceFactory {
  @Bean(name = "businessAttributeService")
  @Scope("singleton")
  @Nonnull
  protected BusinessAttributeService getINSTANCE(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new BusinessAttributeService(entityClient);
  }
}
