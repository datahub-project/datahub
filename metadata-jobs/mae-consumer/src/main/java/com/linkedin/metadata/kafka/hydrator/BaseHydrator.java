package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.RecordTemplate;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class BaseHydrator<SNAPSHOT extends RecordTemplate> {

  /**
   * Use values in the snapshot to hydrate the document
   */
  protected abstract void hydrateFromSnapshot(ObjectNode document, SNAPSHOT snapshot);

}
