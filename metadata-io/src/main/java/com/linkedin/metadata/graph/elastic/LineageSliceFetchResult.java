package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.graph.LineagePartialReason;
import com.linkedin.metadata.graph.LineageRelationship;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Value;

/** Result of a parallel slice fetch (PIT or scroll): merged relationships and partiality. */
@Value
public class LineageSliceFetchResult {
  List<LineageRelationship> lineageRelationships;

  /** Why the fetch is partial; null when it is complete. */
  @Nullable LineagePartialReason partialReason;

  public boolean isPartial() {
    return partialReason != null;
  }
}
