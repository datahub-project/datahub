package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.PathSpec;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Edge {
  private Urn source;
  private Urn destination;
  private String relationshipType;
  private String aspectName;
  private PathSpec pathSpec;
}
