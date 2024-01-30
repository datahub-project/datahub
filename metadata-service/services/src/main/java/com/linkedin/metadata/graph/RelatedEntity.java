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

  /** Urn associated with an entity through which this relationship is established */
  String via;

  /**
   * Constructor for backwards compatibility
   *
   * @param relationshipType
   * @param urn
   */
  public RelatedEntity(String relationshipType, String urn) {
    this(relationshipType, urn, null);
  }
}
