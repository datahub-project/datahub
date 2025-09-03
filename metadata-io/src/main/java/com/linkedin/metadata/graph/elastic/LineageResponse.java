package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.graph.LineageRelationship;
import java.util.List;
import lombok.Value;

/** Response class for lineage queries containing total count and relationships. */
@Value
public class LineageResponse {
  int total;
  List<LineageRelationship> lineageRelationships;
}
