package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DataFlowHydrator implements Hydrator {
  private final Client _restliClient;
  private final RestliRemoteDAO<DataFlowSnapshot, DataFlowAspect, DataFlowUrn> _remoteDAO;

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  public DataFlowHydrator(Client restliClient) {
    _restliClient = restliClient;
    _remoteDAO = new RestliRemoteDAO<>(DataFlowSnapshot.class, DataFlowAspect.class, _restliClient);
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    DataFlowUrn dataFlowUrn;
    try {
      dataFlowUrn = DataFlowUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid DataFlow URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(ORCHESTRATOR, dataFlowUrn.getOrchestratorEntity());

    _remoteDAO.get(DataFlowInfo.class, dataFlowUrn).ifPresent(dataFlowInfo -> jsonObject.put(NAME, dataFlowInfo.getName()));

    return Optional.of(jsonObject);
  }
}
