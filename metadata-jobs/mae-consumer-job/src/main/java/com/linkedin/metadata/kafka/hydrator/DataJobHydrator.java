package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datajob.DataJobKey;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class DataJobHydrator extends Hydrator<DataJobKey> {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromKey(ObjectNode document, DataJobKey key) {
    document.put(ORCHESTRATOR, key.getDataFlow().getOrchestratorEntity());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
    if (!snapshot.isDataJobSnapshot()) {
      log.error("Hydrator {} does not match type of snapshot {}", this.getClass().getSimpleName(),
          snapshot.getClass().getSimpleName());
    }
    for (DataJobAspect aspect : snapshot.getDataJobSnapshot().getAspects()) {
      if (aspect.isDataJobInfo()) {
        document.put(NAME, aspect.getDataJobInfo().getName());
      }
    }
  }
}
