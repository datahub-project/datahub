package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DataJobHydrator extends BaseHydrator<DataJobSnapshot> {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, DataJobSnapshot snapshot) {
    for (DataJobAspect aspect : snapshot.getAspects()) {
      if (aspect.isDataJobInfo()) {
        document.put(NAME, aspect.getDataJobInfo().getName());
      } else if (aspect.isDataJobKey()) {
        try {
          document.put(ORCHESTRATOR,
              DataFlowUrn.createFromString(aspect.getDataJobKey().getFlow().toString()).getOrchestratorEntity());
        } catch (URISyntaxException e) {
          log.info("Failed to parse data flow urn: {}", aspect.getDataJobKey().getFlow());
        }
      }
    }
  }
}
