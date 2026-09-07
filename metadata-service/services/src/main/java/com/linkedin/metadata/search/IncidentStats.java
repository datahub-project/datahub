package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nullable;
import lombok.Value;

/** Active-incident summary for a single entity, produced by a batched aggregation. */
@Value
public class IncidentStats {
  int activeCount;
  @Nullable Urn latestIncidentUrn;
}
