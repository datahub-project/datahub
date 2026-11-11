package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/** A process's periodic look at one upgrade row, run once a minute by the scheduler. */
public interface UpgradeRowPoll {

  /** Reads the row once and records whatever this process owes it; never throws. */
  void poll();

  @Nonnull
  Urn getUpgradeUrn();
}
