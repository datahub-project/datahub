package com.linkedin.metadata.config.pgqueue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import java.util.Optional;

/**
 * Resolves pgQueue SQL poller backoff from {@code postgres.pgQueue.consumerPoll.*} and per-consumer
 * empty-poll max sleeps from {@code mceConsumer}/{@code maeConsumer}/{@code peConsumer} {@code
 * pgQueue.*}. Empty-poll min is shared; max is per topic family.
 */
public final class PgQueueConsumerPollSettings {

  private PgQueueConsumerPollSettings() {}

  public record SleepMillis(long missingTopic, long errorRecovery, long emptyPollMin) {}

  public static SleepMillis requireSleep(PostgresSqlSetupProperties postgresProperties) {
    PostgresSqlSetupProperties.PgQueue.ConsumerPoll poll = consumerPoll(postgresProperties);
    return new SleepMillis(
        requirePositiveLong(
            poll.getMissingTopicSleepMillis(),
            "postgres.pgQueue.consumerPoll.missingTopicSleepMillis"),
        requirePositiveLong(
            poll.getErrorRecoverySleepMillis(),
            "postgres.pgQueue.consumerPoll.errorRecoverySleepMillis"),
        requirePositiveLong(
            poll.getEmptyPollSleepMinMillis(),
            "postgres.pgQueue.consumerPoll.emptyPollSleepMinMillis"));
  }

  public static long requireEmptyPollSleep(Long configured, String propertyName) {
    return requirePositiveLong(configured, propertyName);
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
