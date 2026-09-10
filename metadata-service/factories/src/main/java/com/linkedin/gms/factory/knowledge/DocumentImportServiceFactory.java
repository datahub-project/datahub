package com.linkedin.gms.factory.knowledge;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DocumentImportServiceFactory {

  @Bean(name = "documentImportService")
  @Scope("singleton")
  @Nonnull
  protected DocumentImportService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return new DocumentImportService(systemEntityClient);
  }
}
