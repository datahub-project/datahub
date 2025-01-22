package com.linkedin.gms.factory.actionrequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.dataset.DatasetServiceFactory;
import com.linkedin.gms.factory.domain.DomainServiceFactory;
import com.linkedin.gms.factory.glossary.GlossaryTermServiceFactory;
import com.linkedin.gms.factory.ownership.OwnerServiceFactory;
import com.linkedin.gms.factory.structuredproperty.StructuredPropertyServiceFactory;
import com.linkedin.gms.factory.tag.TagServiceFactory;
import com.linkedin.gms.factory.test.openapi.OpenApiClientFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.DatasetService;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.service.GlossaryTermService;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.service.TagService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({
  DatasetServiceFactory.class,
  TagServiceFactory.class,
  GlossaryTermServiceFactory.class,
  StructuredPropertyServiceFactory.class,
  DomainServiceFactory.class,
  OwnerServiceFactory.class,
  OpenApiClientFactory.class,
  AssertionServiceFactory.class,
})
public class ActionRequestFactory {
  @Bean(name = "actionRequestService")
  @Scope("singleton")
  @Nonnull
  protected ActionRequestService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("graphClient") final GraphClient graphClient,
      @Qualifier("datasetService") final DatasetService datasetService,
      @Qualifier("tagService") final TagService tagService,
      @Qualifier("glossaryTermService") final GlossaryTermService glossaryTermService,
      @Qualifier("structuredPropertyService")
          final StructuredPropertyService structuredPropertyService,
      @Qualifier("domainService") final DomainService domainService,
      @Qualifier("ownerService") final OwnerService ownerService,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new ActionRequestService(
        entityClient,
        entityService,
        graphClient,
        datasetService,
        tagService,
        glossaryTermService,
        structuredPropertyService,
        domainService,
        ownerService,
        openApiClient,
        objectMapper);
  }
}
