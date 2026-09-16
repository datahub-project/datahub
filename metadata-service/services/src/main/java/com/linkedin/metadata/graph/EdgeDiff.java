package com.linkedin.metadata.graph;

import com.linkedin.metadata.aspect.models.graph.Edge;
import java.util.List;
import lombok.Getter;

/** Container class to hold the three types of edge differences. */
@Getter
public class EdgeDiff {
  private final List<Edge> edgesToAdd;
  private final List<Edge> edgesToRemove;
  private final List<Edge> edgesToUpdate;

  public EdgeDiff(List<Edge> edgesToAdd, List<Edge> edgesToRemove, List<Edge> edgesToUpdate) {
    this.edgesToAdd = edgesToAdd;
    this.edgesToRemove = edgesToRemove;
    this.edgesToUpdate = edgesToUpdate;
  }
}
