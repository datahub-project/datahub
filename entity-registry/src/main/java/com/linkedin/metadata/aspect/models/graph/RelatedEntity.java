package com.linkedin.metadata.aspect.models.graph;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Data
public class RelatedEntity {
  /** How the entity is related, along which edge. */
  protected final String relationshipType;

  /** Urn associated with the related entity. */
  protected final String urn;

  /** Urn associated with an entity through which this relationship is established */
  protected final String via;

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
