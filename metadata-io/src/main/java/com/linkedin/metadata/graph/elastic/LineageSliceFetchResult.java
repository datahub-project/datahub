package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.graph.LineageRelationship;
import java.util.List;
import lombok.Value;

/** Result of a parallel slice fetch (PIT or scroll): merged relationships and partiality flag. */
@Value
public class LineageSliceFetchResult {
  List<LineageRelationship> lineageRelationships;
  boolean partial;
}
