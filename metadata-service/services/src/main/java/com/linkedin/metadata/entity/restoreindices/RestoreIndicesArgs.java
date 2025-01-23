package com.linkedin.metadata.entity.restoreindices;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class RestoreIndicesArgs implements Cloneable {
  public static final int DEFAULT_BATCH_SIZE = 500;
  public static final int DEFAULT_NUM_THREADS = 1;
  public static final int DEFAULT_BATCH_DELAY_MS = 1000;
  public static final long DEFAULT_GE_PIT_EPOCH_MS = 0;

  public int start = 0;
  public int batchSize = DEFAULT_BATCH_SIZE;
  public int limit = 0;
  public int numThreads = DEFAULT_NUM_THREADS;
  public long batchDelayMs = DEFAULT_BATCH_DELAY_MS;
  public long gePitEpochMs = DEFAULT_GE_PIT_EPOCH_MS;
  public long lePitEpochMs;
  public String aspectName;
  public List<String> aspectNames = Collections.emptyList();
  public String urn;
  public String urnLike;
  public Boolean urnBasedPagination = false;
  public String lastUrn = "";
  public String lastAspect = "";

  @Override
  public RestoreIndicesArgs clone() {
    try {
      RestoreIndicesArgs clone = (RestoreIndicesArgs) super.clone();
      // TODO: copy mutable state here, so the clone can't change the internals of the original
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }

  public RestoreIndicesArgs start(Integer start) {
    this.start = start != null ? start : 0;
    return this;
  }

  public RestoreIndicesArgs batchSize(Integer batchSize) {
    this.batchSize = batchSize != null ? batchSize : DEFAULT_BATCH_SIZE;
    return this;
  }

  public RestoreIndicesArgs limit(Integer limit) {
    this.limit = limit != null ? limit : 0;
    return this;
  }

  public RestoreIndicesArgs numThreads(Integer numThreads) {
    this.numThreads = numThreads != null ? numThreads : DEFAULT_NUM_THREADS;
    return this;
  }

  public RestoreIndicesArgs batchDelayMs(Long batchDelayMs) {
    this.batchDelayMs = batchDelayMs != null ? batchDelayMs : DEFAULT_BATCH_DELAY_MS;
    return this;
  }

  public RestoreIndicesArgs gePitEpochMs(Long gePitEpochMs) {
    this.gePitEpochMs = gePitEpochMs != null ? gePitEpochMs : DEFAULT_GE_PIT_EPOCH_MS;
    return this;
  }

  public RestoreIndicesArgs lePitEpochMs(Long lePitEpochMs) {
    this.lePitEpochMs = lePitEpochMs != null ? lePitEpochMs : Instant.now().toEpochMilli();
    return this;
  }
}
