package com.linkedin.gms.factory.test.openapi;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiClientFactory {

  @Bean(name = "openApiClient")
  public OpenApiClient openApiClient(
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.openApiClient.maxRetries:3}") int maxRetries,
      @Value("${datahub.gms.openApiClient.backOffMillis:1000}") long backOffMillis,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    return new OpenApiClient(
        gmsHost, gmsPort, gmsUseSSL, systemOperationContext, maxRetries, backOffMillis);
  }
}
