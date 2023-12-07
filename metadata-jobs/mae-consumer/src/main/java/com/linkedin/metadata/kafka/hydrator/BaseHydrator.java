package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.entity.EntityResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseHydrator {

  /** Use values in the entity response to hydrate the document */
  protected abstract void hydrateFromEntityResponse(
      ObjectNode document, EntityResponse entityResponse);
}
