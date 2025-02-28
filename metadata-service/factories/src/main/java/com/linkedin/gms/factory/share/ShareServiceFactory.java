package com.linkedin.gms.factory.share;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ShareService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ShareServiceFactory {

  @Bean(name = "shareService")
  @Nonnull
  protected ShareService getInstance(
      final @Qualifier("systemEntityClient") SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new ShareService(entityClient, openApiClient, objectMapper);
  }
}
