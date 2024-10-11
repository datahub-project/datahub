package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.FormService;
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

  public static Thread assign(
      OperationContext opContext,
      String predicateJson,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient,
      OpenApiClient openApiClient) {
    // TODO: Refactor form service methods to take appSource as a param to avoid having to
    // repeatedly new up instances
    FormService formService =
        new FormService(
            entityClient,
            openApiClient,
            opContext.getObjectMapper(),
            Constants.METADATA_TESTS_SOURCE,
            true);
    Runnable runnable =
        () -> {
          try {
            SearchBasedFormAssignmentManager.apply(
                opContext,
                predicateJson,
                formUrn,
                batchFormEntityCount,
                entityClient,
                formService::batchAssignFormToEntities);
          } catch (Exception e) {
            handleException(e, predicateJson, formUrn, batchFormEntityCount, entityClient);
            throw new RuntimeException("Form assignment runner error.", e);
          }
        };
    Thread assignThread =
        new Thread(
            runnable,
            SearchBasedFormAssignmentRunner.class.getSimpleName() + "-assign-" + nextThreadNum());
    assignThread.start();
    return assignThread;
  }

  public static Thread unassign(
      OperationContext opContext,
      String predicateJson,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient,
      OpenApiClient openApiClient) {
    // TODO: Refactor form service methods to take appSource as a param to avoid having to
    // repeatedly new up instances
    FormService formService =
        new FormService(
            entityClient,
            openApiClient,
            opContext.getObjectMapper(),
            Constants.METADATA_TESTS_SOURCE,
            true);
    Runnable runnable =
        () -> {
          try {
            SearchBasedFormAssignmentManager.apply(
                opContext,
                predicateJson,
                formUrn,
                batchFormEntityCount,
                entityClient,
                formService::batchUnassignFormForEntities);
          } catch (Exception e) {
            handleException(e, predicateJson, formUrn, batchFormEntityCount, entityClient);
          }
        };
    Thread unassignThread =
        new Thread(
            runnable,
            SearchBasedFormAssignmentRunner.class.getSimpleName() + "-unassign-" + nextThreadNum());
    unassignThread.start();
    return unassignThread;
  }

  private static void handleException(
      Exception e,
      String predicateJson,
      Urn formUrn,
      int batchFormEntityCount,
      EntityClient entityClient) {
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
