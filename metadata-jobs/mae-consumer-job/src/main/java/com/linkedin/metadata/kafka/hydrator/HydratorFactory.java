package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.restli.client.Client;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class HydratorFactory {
  private final Client _restliClient;
  private final Map<EntityType, Hydrator> _hydratorMap;

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public HydratorFactory(Client restliClient) {
    _restliClient = restliClient;
    _hydratorMap = new HashMap<>();
    _hydratorMap.put(EntityType.DATASET, new DatasetHydrator());
    _hydratorMap.put(EntityType.CHART, new ChartHydrator(_restliClient));
    _hydratorMap.put(EntityType.DASHBOARD, new DashboardHydrator(_restliClient));
    _hydratorMap.put(EntityType.DATA_JOB, new DataJobHydrator(_restliClient));
    _hydratorMap.put(EntityType.DATA_FLOW, new DataFlowHydrator(_restliClient));
    _hydratorMap.put(EntityType.CORP_USER, new CorpUserHydrator(_restliClient));
  }

  public Optional<ObjectNode> getHydratedEntity(EntityType entityType, String urn) {
    if (!_hydratorMap.containsKey(entityType)) {
      return Optional.empty();
    }
    return _hydratorMap.get(entityType).getHydratedEntity(urn);
  }
}
