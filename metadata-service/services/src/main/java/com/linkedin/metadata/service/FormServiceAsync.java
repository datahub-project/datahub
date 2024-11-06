package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;

public class FormServiceAsync extends FormService {
  public FormServiceAsync(
      @Nonnull SystemEntityClient systemEntityClient,
      @Nonnull OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      @Nonnull final String appSource) {
    super(systemEntityClient, openApiClient, objectMapper, appSource, true);
  }
}
