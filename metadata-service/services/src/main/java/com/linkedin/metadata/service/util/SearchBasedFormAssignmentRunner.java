package com.linkedin.metadata.service.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.utils.elasticsearch.AcrylSearchUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchBasedFormAssignmentRunner {

  private static FormService formService = null;

  private static final AtomicLong THREAD_INIT_NUMBER = new AtomicLong();

  private static long nextThreadNum() {
    return THREAD_INIT_NUMBER.getAndIncrement();
  }

  public static void assign(
      OperationContext opContext,
      String predicateJson,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient,
      OpenApiClient openApiClient,
      ObjectMapper objectMapper) {
    // TODO: Refactor form service methods to take appSource as a param to avoid having to repeatedly new up instances
    FormService formService =
        new FormService(
            entityClient, openApiClient, objectMapper, Constants.METADATA_TESTS_SOURCE, true);
    Runnable runnable = () -> {
      try {
        SearchBasedFormAssignmentManager.apply(
            opContext,
            predicateJson,
            formUrn,
            batchFormEntityCount,
            entityClient,
            formService::batchAssignFormToEntities
            );
      } catch (Exception e) {
        handleException(e, predicateJson, formUrn, batchFormEntityCount, entityClient);
        throw new RuntimeException("Form assignment runner error.", e);
      }
    };
    new Thread(runnable, SearchBasedFormAssignmentRunner.class.getSimpleName() + "-assign-" + nextThreadNum()).start();
  }

  public static void unassign(
      OperationContext opContext,
      String predicateJson,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient,
      OpenApiClient openApiClient,
      ObjectMapper objectMapper) {
    // TODO: Refactor form service methods to take appSource as a param to avoid having to repeatedly new up instances
    FormService formService =
        new FormService(
            entityClient, openApiClient, objectMapper, Constants.METADATA_TESTS_SOURCE, true);
    Runnable runnable = () -> {
      try {
        SearchBasedFormAssignmentManager.apply(
            opContext,
            predicateJson,
            formUrn,
            batchFormEntityCount,
            entityClient,
            formService::batchUnassignFormForEntities
        );
      } catch (Exception e) {
        handleException(e, predicateJson, formUrn, batchFormEntityCount, entityClient);
      }
    };
    new Thread(runnable, SearchBasedFormAssignmentRunner.class.getSimpleName() + "-unassign-" + nextThreadNum()).start();
  }

  private static void handleException(Exception e, String predicateJson, Urn formUrn, int batchFormEntityCount, EntityClient entityClient) {
    log.error(
        "SearchBasedFormAssignmentRunner failed to run. "
            + "Options: formFilters: {}, "
            + "formUrn: {}, "
            + "batchFormCount: {}, "
            + "entityClient: {}, ",
        predicateJson,
        formUrn,
        batchFormEntityCount,
        entityClient);
    throw new RuntimeException("Form assignment runner error.", e);
  }

  private SearchBasedFormAssignmentRunner() {}
}
