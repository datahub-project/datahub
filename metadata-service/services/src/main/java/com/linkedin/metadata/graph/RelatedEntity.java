package com.linkedin.metadata.graph;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RelatedEntity {
  /** How the entity is related, along which edge. */
  String relationshipType;

  /** Urn associated with the related entity. */
  String urn;
}
