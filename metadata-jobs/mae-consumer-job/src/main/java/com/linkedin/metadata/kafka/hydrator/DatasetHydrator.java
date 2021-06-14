package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class DatasetHydrator extends Hydrator<DatasetKey> {

  private static final String PLATFORM = "platform";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromKey(ObjectNode document, DatasetKey key) {
    document.put(PLATFORM, key.getPlatform().toString());
    document.put(NAME, key.getName());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
  }
}
