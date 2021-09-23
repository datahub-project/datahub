package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DataFlowHydrator extends BaseHydrator<DataFlowSnapshot> {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, DataFlowSnapshot snapshot) {
    for (DataFlowAspect aspect : snapshot.getAspects()) {
      if (aspect.isDataFlowInfo()) {
        document.put(NAME, aspect.getDataFlowInfo().getName());
      } else if (aspect.isDataFlowKey()) {
        document.put(ORCHESTRATOR, aspect.getDataFlowKey().getOrchestrator());
      }
    }
  }
}
