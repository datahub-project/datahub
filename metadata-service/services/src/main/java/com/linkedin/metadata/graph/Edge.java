package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
public class Edge {
  @EqualsAndHashCode.Include private Urn source;
  @EqualsAndHashCode.Include private Urn destination;
  @EqualsAndHashCode.Include private String relationshipType;
  @EqualsAndHashCode.Exclude private Long createdOn;
  @EqualsAndHashCode.Exclude private Urn createdActor;
  @EqualsAndHashCode.Exclude private Long updatedOn;
  @EqualsAndHashCode.Exclude private Urn updatedActor;
  @EqualsAndHashCode.Exclude private Map<String, Object> properties;
}
