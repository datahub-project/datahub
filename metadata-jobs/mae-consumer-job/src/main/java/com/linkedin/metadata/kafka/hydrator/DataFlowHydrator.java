package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class DataFlowHydrator extends Hydrator<DataFlowKey> {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromKey(ObjectNode document, DataFlowKey key) {
    document.put(ORCHESTRATOR, key.getOrchestrator());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
    if (!snapshot.isDataFlowSnapshot()) {
      log.error("Hydrator {} does not match type of snapshot {}", this.getClass().getSimpleName(),
          snapshot.getClass().getSimpleName());
    }
    for (DataFlowAspect aspect : snapshot.getDataFlowSnapshot().getAspects()) {
      if (aspect.isDataFlowInfo()) {
        document.put(NAME, aspect.getDataFlowInfo().getName());
      }
    }
  }
}
