package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import lombok.Value;

@Value
public class IngestProposalResult {
  Urn urn;
  boolean didUpdate;
  boolean queued;
}
