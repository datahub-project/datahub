package com.linkedin.datahub.graphql;

import com.linkedin.metadata.query.filter.RelationshipDirection;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RelationshipKey {
  private String urn;
  private String relationshipName;
  private RelationshipDirection direction; // optional.
}
