package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Edge {
  private Urn source;
  private Urn destination;
  private String relationshipType;
  // For no-code filtering
  //private Map<String, Object> metadata;
  private EdgeMetadata metadata;
}
