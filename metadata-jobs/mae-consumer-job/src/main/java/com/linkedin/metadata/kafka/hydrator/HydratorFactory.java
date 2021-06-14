package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class HydratorFactory {
  private final Map<EntityType, Hydrator<?>> _hydratorMap;

  public HydratorFactory(EntityClient entityClient, EntityRegistry entityRegistry) {
    _hydratorMap = new HashMap<>();
    _hydratorMap.put(EntityType.DATASET,
        DatasetHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
    _hydratorMap.put(EntityType.CHART,
        ChartHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
    _hydratorMap.put(EntityType.DASHBOARD,
        DashboardHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
    _hydratorMap.put(EntityType.DATA_JOB,
        DataJobHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
    _hydratorMap.put(EntityType.DATA_FLOW,
        DataFlowHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
    _hydratorMap.put(EntityType.CORP_USER,
        CorpUserHydrator.builder().entityClient(entityClient).entityRegistry(entityRegistry).build());
  }

  public Optional<ObjectNode> getHydratedEntity(EntityType entityType, String urn) {
    if (!_hydratorMap.containsKey(entityType)) {
      return Optional.empty();
    }
    return _hydratorMap.get(entityType).getHydratedEntity(urn);
  }
}
