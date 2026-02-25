package com.linkedin.gms.factory.knowledge;

import com.linkedin.entity.client.SystemEntityClient;
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
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return new DocumentService(systemEntityClient);
  }
}
