package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
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
      EntityClient entityClient,
      OpenApiClient openApiClient) {
    Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            try {
              SearchBasedFormAssignmentManager.apply(
                  opContext, formFilters, formUrn, batchFormEntityCount, entityClient, openApiClient);
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
