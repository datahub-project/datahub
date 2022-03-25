package com.linkedin.metadata.graph;

import com.linkedin.data.schema.PathSpec;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RelatedEntity {
  /**
   * How the entity is related, along which edge.
   */
  String relationshipType;

  /**
   * Urn associated with the related entity.
   */
  String urn;

  /**
   * Aspect containing the relationshipType
   */
  String aspectName;

  /**
   * Path spec of the relationship from the aspect name to the related entity.
   */
  PathSpec spec;
}
