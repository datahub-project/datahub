package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;

public class OwnerServiceAsync extends OwnerService {
  public OwnerServiceAsync(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper, true);
  }
}
