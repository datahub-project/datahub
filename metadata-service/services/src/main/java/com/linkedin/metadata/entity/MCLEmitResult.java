package com.linkedin.metadata.entity;

import com.linkedin.mxe.MetadataChangeLog;
import java.util.concurrent.Future;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class MCLEmitResult {
  MetadataChangeLog metadataChangeLog;

  // The result when written to MCL Topic
  Future<?> mclFuture;

  // Whether this was preprocessed before being emitted
  boolean processedMCL;

  // Set to true if the message was emitted, false if this was dropped due to some config.
  boolean emitted;
}
