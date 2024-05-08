package com.linkedin.metadata.service.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchBasedFormAssignmentRunner {

  public static void assign(
      OperationContext opContext,
      DynamicFormAssignment formFilters,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient,
      OpenApiClient openApiClient,
      ObjectMapper objectMapper) {
    Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            try {
              SearchBasedFormAssignmentManager.apply(
                  opContext,
                  formFilters,
                  formUrn,
                  batchFormEntityCount,
                  entityClient,
                  openApiClient,
                  objectMapper);
            } catch (Exception e) {
              log.error(
                  "SearchBasedFormAssignmentRunner failed to run. "
                      + "Options: formFilters: {}, "
                      + "formUrn: {}, "
                      + "batchFormCount: {}, "
                      + "entityClient: {}, ",
                  formFilters,
                  formUrn,
                  batchFormEntityCount,
                  entityClient);
              throw new RuntimeException("Form assignment runner error.", e);
            }
          }
        };

    new Thread(runnable).start();
  }

  private SearchBasedFormAssignmentRunner() {}
}
