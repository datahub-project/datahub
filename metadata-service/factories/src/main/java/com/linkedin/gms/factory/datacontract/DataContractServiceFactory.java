package com.linkedin.gms.factory.datacontract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.DataContractService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataContractServiceFactory {
  @Bean(name = "dataContractService")
  @Nonnull
  protected DataContractService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("graphClient") final GraphClient graphClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new DataContractService(systemEntityClient, graphClient, openApiClient, objectMapper);
  }
}
