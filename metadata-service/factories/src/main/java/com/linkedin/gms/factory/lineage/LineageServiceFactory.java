package com.linkedin.gms.factory.lineage;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.LineageService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class LineageServiceFactory {

  @Bean(name = "lineageService")
  @Scope("singleton")
  @Nonnull
  protected LineageService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient) throws Exception {
    return new LineageService(entityClient);
  }
}
