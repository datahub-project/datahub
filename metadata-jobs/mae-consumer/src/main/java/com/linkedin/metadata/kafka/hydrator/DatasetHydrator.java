package com.linkedin.metadata.kafka.hydrator;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DatasetKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetHydrator extends BaseHydrator {

  private static final String PLATFORM = "platform";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(DATASET_KEY_ASPECT_NAME, this::mapKey);
  }

  private void mapKey(ObjectNode jsonNodes, DataMap dataMap) {
    DatasetKey datasetKey = new DatasetKey(dataMap);
    jsonNodes.put(PLATFORM, datasetKey.getPlatform().toString());
    jsonNodes.put(NAME, datasetKey.getName());
  }
}
