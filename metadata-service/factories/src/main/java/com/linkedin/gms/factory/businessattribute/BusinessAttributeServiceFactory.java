package com.linkedin.gms.factory.businessattribute;

import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.BusinessAttributeService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
public class BusinessAttributeServiceFactory {
  private final JavaEntityClient entityClient;

  public BusinessAttributeServiceFactory(
      @Qualifier("javaEntityClient") JavaEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  @Bean(name = "businessAttributeService")
  @Scope("singleton")
  @Nonnull
  protected BusinessAttributeService getINSTANCE() throws Exception {
    return new BusinessAttributeService(entityClient);
  }
}
