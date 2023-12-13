package com.linkedin.metadata.kafka.hydrator;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataFlowKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataFlowHydrator extends BaseHydrator {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(
        DATA_FLOW_INFO_ASPECT_NAME,
        (jsonNodes, dataMap) -> jsonNodes.put(NAME, new DataFlowInfo(dataMap).getName()));
    mappingHelper.mapToResult(
        CORP_USER_KEY_ASPECT_NAME,
        (jsonNodes, dataMap) ->
            jsonNodes.put(ORCHESTRATOR, new DataFlowKey(dataMap).getOrchestrator()));
  }
}
