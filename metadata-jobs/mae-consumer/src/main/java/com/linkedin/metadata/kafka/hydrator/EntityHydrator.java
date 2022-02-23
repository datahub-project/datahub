package com.linkedin.metadata.kafka.hydrator;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class EntityHydrator {

  private final Authentication _systemAuthentication;
  private final EntityClient _entityClient;

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
    Entity entity;
    try {
      entity = (Entity) _entityClient.get(urnObj, this._systemAuthentication);
    } catch (RemoteInvocationException e) {
      log.error("Error while calling GMS to hydrate entity for urn {}", urn);
      e.printStackTrace();
      return Optional.empty();
    }

    Snapshot snapshot = entity.getValue();
    if (snapshot.isChartSnapshot()) {
      _chartHydrator.hydrateFromSnapshot(document, snapshot.getChartSnapshot());
    } else if (snapshot.isCorpUserSnapshot()) {
      _corpUserHydrator.hydrateFromSnapshot(document, snapshot.getCorpUserSnapshot());
    } else if (snapshot.isDashboardSnapshot()) {
      _dashboardHydrator.hydrateFromSnapshot(document, snapshot.getDashboardSnapshot());
    } else if (snapshot.isDataFlowSnapshot()) {
      _dataFlowHydrator.hydrateFromSnapshot(document, snapshot.getDataFlowSnapshot());
    } else if (snapshot.isDataJobSnapshot()) {
      _dataJobHydrator.hydrateFromSnapshot(document, snapshot.getDataJobSnapshot());
    } else if (snapshot.isDatasetSnapshot()) {
      _datasetHydrator.hydrateFromSnapshot(document, snapshot.getDatasetSnapshot());
    }
    return Optional.of(document);
  }
}
