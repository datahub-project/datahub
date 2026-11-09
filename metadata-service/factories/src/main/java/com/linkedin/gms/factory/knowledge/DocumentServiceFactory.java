package com.linkedin.gms.factory.knowledge;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.service.DocumentService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DocumentServiceFactory {

  @Bean(name = "documentService")
  @Scope("singleton")
  @Nonnull
  protected DocumentService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      final ConfigurationProvider configurationProvider) {
    final boolean searchV3 =
        EntitySearchIndexResolver.shouldReadV3(
            configurationProvider.getElasticSearch().getEntityIndex());
    return new DocumentService(
        systemEntityClient, searchV3 ? MappingConstants.SYSTEM_CREATED_TIME_FIELD : "createdAt");
  }
}
