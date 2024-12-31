package com.linkedin.entity.client;

import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class EntityClientConfig {
  @Nonnull @Builder.Default BackoffPolicy backoffPolicy = new ExponentialBackoff(2);
  @Builder.Default int retryCount = 3;
  @Builder.Default int batchGetV2Size = 5;
  @Builder.Default int batchGetV2Concurrency = 1;
  @Builder.Default int batchGetV2QueueSize = 100;
  @Builder.Default int batchGetV2KeepAlive = 60;
  @Builder.Default int batchIngestSize = 5;
  @Builder.Default int batchIngestConcurrency = 1;
  @Builder.Default int batchIngestQueueSize = 100;
  @Builder.Default int batchIngestKeepAlive = 60;

  public int getBatchGetV2Size() {
    return Math.max(1, batchGetV2Size);
  }

  public int getBatchGetV2Concurrency() {
    return Math.max(1, batchGetV2Concurrency);
  }

  public int getBatchIngestSize() {
    return Math.max(1, batchIngestSize);
  }

  public int getBatchIngestConcurrency() {
    return Math.max(1, batchIngestConcurrency);
  }

  public int getBatchIngestQueueSize() {
    return Math.max(1, batchIngestQueueSize);
  }

  public int getBatchGetV2QueueSize() {
    return Math.max(1, batchGetV2QueueSize);
  }
}
