package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DatasetUrn;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatasetHydrator implements Hydrator {

  private static final String PLATFORM = "platform";
  private static final String NAME = "name";

  public DatasetHydrator() {
  }

  @Override
  public Optional<ObjectNode> getHydratedEntity(String urn) {
    DatasetUrn datasetUrn;
    try {
      datasetUrn = DatasetUrn.createFromString(urn);
    } catch (URISyntaxException e) {
      log.info("Invalid Dataset URN: {}", urn);
      return Optional.empty();
    }

    ObjectNode jsonObject = HydratorFactory.OBJECT_MAPPER.createObjectNode();
    jsonObject.put(PLATFORM, datasetUrn.getPlatformEntity().getPlatformNameEntity());
    jsonObject.put(NAME, datasetUrn.getDatasetNameEntity());
    return Optional.of(jsonObject);
  }
}
