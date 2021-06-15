package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatasetHydrator extends BaseHydrator<DatasetSnapshot> {

  private static final String PLATFORM = "platform";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, DatasetSnapshot snapshot) {
    for (DatasetAspect aspect : snapshot.getAspects()) {
      if (aspect.isDatasetKey()) {
        document.put(PLATFORM, aspect.getDatasetKey().getPlatform().toString());
        document.put(NAME, aspect.getDatasetKey().getName());
      }
    }
  }
}
