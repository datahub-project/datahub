/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeseries;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericTimeseriesDocument {
  @Nonnull private String urn;
  private long timestampMillis;

  @JsonProperty("@timestamp")
  private long timestamp;

  @Nonnull private Object event;
  @Nullable private String messageId;
  @Nullable private Object systemMetadata;
  @Nullable private String eventGranularity;
  private boolean isExploded;
  @Nullable private String runId;
  @Nullable private String partition;
  @Nullable private Object partitionSpec;
}
