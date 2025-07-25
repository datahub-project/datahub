package com.linkedin.gms.factory.actionrequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.dataproduct.DataProductServiceFactory;
import com.linkedin.gms.factory.domain.DomainServiceFactory;
import com.linkedin.gms.factory.ownership.OwnerServiceFactory;
import com.linkedin.gms.factory.test.openapi.OpenApiClientFactory;
import com.linkedin.gms.factory.user.UserServiceFactory;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.service.UserService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({
  DomainServiceFactory.class,
  OwnerServiceFactory.class,
  DataProductServiceFactory.class,
  UserServiceFactory.class,
  OpenApiClientFactory.class,
})
public class ActionWorkflowServiceFactory {
  @Bean(name = "actionWorkflowService")
  @Scope("singleton")
  @Nonnull
  protected ActionWorkflowService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      @Qualifier("ownerService") final OwnerService ownerService,
      @Qualifier("domainService") final DomainService domainService,
      @Qualifier("dataProductService") final DataProductService dataProductService,
      @Qualifier("userService") final UserService userService,
      final ObjectMapper objectMapper)
      throws Exception {
    return new ActionWorkflowService(
        entityClient,
        openApiClient,
        objectMapper,
        ownerService,
        domainService,
        dataProductService,
        userService);
  }
}
