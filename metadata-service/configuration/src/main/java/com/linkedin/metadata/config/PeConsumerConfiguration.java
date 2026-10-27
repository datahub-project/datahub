package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Runtime tuning for {@code metadata-jobs/pe-consumer-job}. Defaults and env-backed placeholders
 * live only in {@code application.yaml} ({@code peConsumer.*}).
 */
@Data
public class PeConsumerConfiguration {

  private PgQueuePoll pgQueue;

  /** pgQueue SQL poll worker settings for platform event processing. */
  @Data
  public static class PgQueuePoll {
    /** Max rows per poll for {@code PlatformEventProcessor}. */
    private Integer platformEventMaxBatch;

    /**
     * Maximum empty-poll sleep for platform-event workers. Exponential backoff starts at {@code
     * postgres.pgQueue.consumerPoll.emptyPollSleepMinMillis}.
     */
    private Long platformEventEmptyPollSleepMillis;
  }
}
