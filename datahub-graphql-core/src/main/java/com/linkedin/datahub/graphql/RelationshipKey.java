package com.linkedin.datahub.graphql;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class RelationshipKey {
  private List<String> incomingRelationshipTypes;
  private List<String> outgoingRelationshipTypes;
  private String urn;
}
