package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DataJobHydrator implements Hydrator {
  private final Client _restliClient;
  private final RestliRemoteDAO<DataJobSnapshot, DataJobAspect, DataJobUrn> _remoteDAO;

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  public DataJobHydrator(Client restliClient) {
    _restliClient = restliClient;
    _remoteDAO = new RestliRemoteDAO<>(DataJobSnapshot.class, DataJobAspect.class, _restliClient);
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    DataJobUrn dataJobUrn;
    try {
      dataJobUrn = DataJobUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid DataJob URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(ORCHESTRATOR, dataJobUrn.getFlowEntity().getOrchestratorEntity());

    _remoteDAO.get(DataJobInfo.class, dataJobUrn).ifPresent(dataJobInfo -> jsonObject.put(NAME, dataJobInfo.getName()));

    return Optional.of(jsonObject);
  }
}
