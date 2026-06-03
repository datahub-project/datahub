package com.linkedin.metadata.config.pgqueue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import java.util.Optional;

/** Resolves pgQueue SQL poller sleep/backoff from {@code postgres.pgQueue.consumerPoll.*}. */
public final class PgQueueConsumerPollSettings {

  private PgQueueConsumerPollSettings() {}

  public record SleepMillis(long emptyPoll, long missingTopic, long errorRecovery) {}

  public static SleepMillis requireSleep(PostgresSqlSetupProperties postgresProperties) {
    PostgresSqlSetupProperties.PgQueue.ConsumerPoll poll = consumerPoll(postgresProperties);
    return new SleepMillis(
        requirePositiveLong(
            poll.getEmptyPollSleepMillis(), "postgres.pgQueue.consumerPoll.emptyPollSleepMillis"),
        requirePositiveLong(
            poll.getMissingTopicSleepMillis(),
            "postgres.pgQueue.consumerPoll.missingTopicSleepMillis"),
        requirePositiveLong(
            poll.getErrorRecoverySleepMillis(),
            "postgres.pgQueue.consumerPoll.errorRecoverySleepMillis"));
  }

  /** MCL pollers use a shorter empty-poll sleep than other pipelines when configured. */
  public static long requireMclEmptyPollSleep(PostgresSqlSetupProperties postgresProperties) {
    PostgresSqlSetupProperties.PgQueue.ConsumerPoll poll = consumerPoll(postgresProperties);
    Long mclEmpty = poll.getMclEmptyPollSleepMillis();
    if (mclEmpty != null) {
      return requirePositiveLong(mclEmpty, "postgres.pgQueue.consumerPoll.mclEmptyPollSleepMillis");
    }
    return requireSleep(postgresProperties).emptyPoll();
  }

  public static int requirePollMaxBatch(Integer configured, String propertyName) {
    if (configured == null) {
      throw new IllegalStateException(propertyName + " must be set in application.yaml");
    }
    if (configured < 1) {
      throw new IllegalStateException(propertyName + " must be >= 1, got " + configured);
    }
    return configured;
  }

  private static PostgresSqlSetupProperties.PgQueue.ConsumerPoll consumerPoll(
      PostgresSqlSetupProperties postgresProperties) {
    return Optional.ofNullable(postgresProperties)
        .map(PostgresSqlSetupProperties::getPgQueue)
        .map(PostgresSqlSetupProperties.PgQueue::getConsumerPoll)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "postgres.pgQueue.consumerPoll must be set in application.yaml"));
  }

  private static long requirePositiveLong(Long value, String propertyName) {
    if (value == null) {
      throw new IllegalStateException(propertyName + " must be set in application.yaml");
    }
    if (value < 1) {
      throw new IllegalStateException(propertyName + " must be >= 1, got " + value);
    }
    return value;
  }
}
