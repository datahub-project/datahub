package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.graph.LineagePartialReason;
import com.linkedin.metadata.graph.LineageRelationship;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Value;

/** Response class for lineage queries containing total count and relationships. */
@Value
public class LineageResponse {
  int total;
  List<LineageRelationship> lineageRelationships;

  /** Why the response is partial; null when it is complete. */
  @Nullable LineagePartialReason partialReason;

  public boolean isPartial() {
    return partialReason != null;
  }
}
