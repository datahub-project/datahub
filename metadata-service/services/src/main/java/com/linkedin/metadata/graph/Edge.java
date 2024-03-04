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
  // The entity who owns the lifecycle of this edge
  @EqualsAndHashCode.Exclude private Urn lifecycleOwner;
  // An entity through which the edge between source and destination is created
  @EqualsAndHashCode.Include private Urn via;

  // For backwards compatibility
  public Edge(
      Urn source,
      Urn destination,
      String relationshipType,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      Map<String, Object> properties) {
    this(
        source,
        destination,
        relationshipType,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        properties,
        null,
        null);
  }
}
