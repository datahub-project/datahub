/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity;

import com.linkedin.mxe.MetadataChangeLog;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class MCLEmitResult {
  MetadataChangeLog metadataChangeLog;

  // The result when written to MCL Topic
  Future<?> mclFuture;

  // Whether the mcl was successfully written to the destination topic
  boolean isProduced() {
    if (mclFuture != null) {
      try {
        mclFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }
  ;

  // Whether this was preprocessed before being emitted
  boolean processedMCL;

  // Set to true if the message was emitted, false if this was dropped due to some config.
  boolean emitted;
}
