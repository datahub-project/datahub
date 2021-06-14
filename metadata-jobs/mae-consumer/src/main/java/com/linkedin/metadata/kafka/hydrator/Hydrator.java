package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;


public interface Hydrator {
  Optional<ObjectNode> getHydratedEntity(String urn);
}
