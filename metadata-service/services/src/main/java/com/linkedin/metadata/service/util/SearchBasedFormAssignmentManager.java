package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics.TAG_OPERATION;
import static com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics.TAG_PHASE;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchBasedFormAssignmentManager {

  static final String METRIC_PREFIX = "datahub.forms.assignment";
  static final String OPERATION_TYPE = "searchBasedFormAssignment";
  static final String PHASE_ASSIGN = "assign";

  private static final ImmutableList<String> ENTITY_TYPES =
      ImmutableList.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CORP_USER_ENTITY_NAME,
          Constants.CORP_GROUP_ENTITY_NAME,
          Constants.DOMAIN_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME,
          Constants.GLOSSARY_TERM_ENTITY_NAME,
          Constants.GLOSSARY_NODE_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME,
          Constants.ML_PRIMARY_KEY_ENTITY_NAME,
          Constants.DATA_PRODUCT_ENTITY_NAME);

  public static void apply(final FormAssignmentScrollRequest request) throws Exception {
    final OperationContext opContext = request.getOpContext();
    final DynamicFormAssignment formFilters = request.getFormFilters();
    final Urn formUrn = request.getFormUrn();
    final int batchFormEntityCount = request.getBatchFormEntityCount();
    final SystemEntityClient entityClient = request.getEntityClient();

    final LongRunningOperationMetrics metrics =
        LongRunningOperationMetrics.begin(
            opContext.getMetricUtils().orElse(null),
            METRIC_PREFIX,
            Tags.of(TAG_OPERATION, OPERATION_TYPE, TAG_PHASE, PHASE_ASSIGN));

    try {
      int totalResults = 0;
      String scrollId = null;
      FormService formService = new FormService(entityClient);

      do {

        ScrollResult results =
            entityClient.scrollAcrossEntities(
                opContext,
                ENTITY_TYPES,
                "*",
                formFilters.getFilter(),
                scrollId,
                "5m",
                List.of(),
                batchFormEntityCount);

        if (!results.hasEntities()
            || results.getNumEntities() == 0
            || results.getEntities().isEmpty()) {
          break;
        }

        log.info("Search across entities results: {}.", results);

        final List<Urn> entityUrns =
            results.getEntities().stream()
                .map(SearchEntity::getEntity)
                .collect(Collectors.toList());

        formService.batchAssignFormToEntities(opContext, entityUrns, formUrn);

        metrics.recordEntities(entityUrns.size());
        metrics.recordPage();

        log.info("Batch assign {} entities to form {}.", entityUrns.size(), formUrn);

        totalResults += results.getEntities().size();
        scrollId = results.getScrollId();

        log.info(
            "Starting batch assign forms, count: {} running total: {}, size: {}",
            batchFormEntityCount,
            totalResults,
            results.getEntities().size());

      } while (scrollId != null);

      log.info("Successfully assigned {} entities to form {}.", totalResults, formUrn);

    } catch (RemoteInvocationException e) {
      metrics.failed("remote_invocation");
      log.error("Error while assigning form to entities.", e);
      // Wrap preserved: runner + callers treat this path as RuntimeException; inspect getCause()
      // if the typed RemoteInvocationException is needed upstream.
      throw new RuntimeException(e);
    } catch (Exception e) {
      // FormService.verifyEntitiesExist wraps client RIEs in RuntimeException — unwrap so the
      // error taxonomy stays remote_invocation instead of unexpected.
      if (isRemoteInvocationFailure(e)) {
        metrics.failed("remote_invocation");
        log.error("Error while assigning form to entities.", e);
      } else {
        metrics.failed("unexpected");
        log.error("Unexpected error while assigning form to entities.", e);
      }
      throw e;
    } finally {
      metrics.finish();
    }
  }

  static boolean isRemoteInvocationFailure(final Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof RemoteInvocationException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private SearchBasedFormAssignmentManager() {}
}
