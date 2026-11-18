package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/** An ingestion source's newest execution requests, produced by a batched aggregation. */
@Value
public class LatestIngestionRuns {
  @Nonnull Urn latestRun;
  @Nullable Urn latestSuccessfulRun;
  @Nullable Urn latestFailedRun;
}
