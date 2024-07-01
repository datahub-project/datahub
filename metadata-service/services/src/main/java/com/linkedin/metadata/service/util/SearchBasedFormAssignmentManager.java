package com.linkedin.metadata.service.util;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.service.FormService;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchBasedFormAssignmentManager {

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
          Constants.DATA_PRODUCT_ENTITY_NAME,
          Constants.SCHEMA_FIELD_ENTITY_NAME);

  public static void apply(
      OperationContext opContext,
      DynamicFormAssignment formFilters,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient)
      throws Exception {

    try {
      int totalResults = 0;
      int numResults = 0;
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
                batchFormEntityCount);

        if (!results.hasEntities()
            || results.getNumEntities() == 0
            || results.getEntities().isEmpty()) {
          break;
        }

        log.info("Search across entities results: {}.", results);

        if (results.hasEntities()) {
          final List<Urn> entityUrns =
              results.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toList());

          formService.batchAssignFormToEntities(opContext, entityUrns, formUrn);

          if (!entityUrns.isEmpty()) {
            log.info("Batch assign {} entities to form {}.", entityUrns.size(), formUrn);
          }

          numResults = results.getEntities().size();
          totalResults += numResults;
          scrollId = results.getScrollId();

          log.info(
              "Starting batch assign forms, count: {} running total: {}, size: {}",
              batchFormEntityCount,
              totalResults,
              results.getEntities().size());

        } else {
          break;
        }
      } while (scrollId != null);

      log.info("Successfully assigned {} entities to form {}.", totalResults, formUrn);

    } catch (RemoteInvocationException e) {
      log.error("Error while assigning form to entities.", e);
      throw new RuntimeException(e);
    }
  }

  private SearchBasedFormAssignmentManager() {}
}
