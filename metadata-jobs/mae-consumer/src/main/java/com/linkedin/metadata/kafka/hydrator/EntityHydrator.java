package com.linkedin.metadata.kafka.hydrator;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.config.EntityHydratorConfig.EXCLUDED_ASPECTS;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class EntityHydrator {

  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final ChartHydrator _chartHydrator = new ChartHydrator();
  private final CorpUserHydrator _corpUserHydrator = new CorpUserHydrator();
  private final DashboardHydrator _dashboardHydrator = new DashboardHydrator();
  private final DataFlowHydrator _dataFlowHydrator = new DataFlowHydrator();
  private final DataJobHydrator _dataJobHydrator = new DataJobHydrator();
  private final DatasetHydrator _datasetHydrator = new DatasetHydrator();

  public Optional<ObjectNode> getHydratedEntity(String urn) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();
    // Hydrate fields from urn
    Urn urnObj;
    try {
      urnObj = Urn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid URN: {}", urn);
      return Optional.empty();
    }
    // Hydrate fields from snapshot
    EntityResponse entityResponse;
    try {
      Set<String> aspectNames =
          Optional.ofNullable(
                  systemOperationContext
                      .getEntityRegistry()
                      .getEntitySpecs()
                      .get(urnObj.getEntityType()))
              .map(
                  spec ->
                      spec.getAspectSpecs().stream()
                          .map(AspectSpec::getName)
                          .filter(aspectName -> !EXCLUDED_ASPECTS.contains(aspectName))
                          .collect(Collectors.toSet()))
              .orElse(Set.of());
      entityResponse = entityClient.getV2(systemOperationContext, urnObj, aspectNames);
    } catch (RemoteInvocationException | URISyntaxException e) {
      log.error("Error while calling GMS to hydrate entity for urn {}", urn);
      return Optional.empty();
    }

    if (entityResponse == null) {
      log.error("Could not find entity for urn {}", urn);
      return Optional.empty();
    }

    switch (entityResponse.getEntityName()) {
      case CHART_ENTITY_NAME:
        _chartHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      case CORP_USER_ENTITY_NAME:
        _corpUserHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      case DASHBOARD_ENTITY_NAME:
        _dashboardHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      case DATA_FLOW_ENTITY_NAME:
        _dataFlowHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      case DATA_JOB_ENTITY_NAME:
        _dataJobHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      case DATASET_ENTITY_NAME:
        _datasetHydrator.hydrateFromEntityResponse(document, entityResponse);
        break;
      default:
        log.error(
            "Unable to find valid hydrator for entity type: {} urn: {}",
            entityResponse.getEntityName(),
            urn);
        return Optional.empty();
    }
    return Optional.of(document);
  }
}
