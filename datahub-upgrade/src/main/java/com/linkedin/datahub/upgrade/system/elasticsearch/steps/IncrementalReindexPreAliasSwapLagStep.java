package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.MclConsumerLagUtils;
import com.linkedin.gms.factory.kafka.common.AdminClientFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Before automatic alias swap, checks combined median MCL lag against {@code
 * elasticsearch.buildIndices.preAliasSwapMaxMclLagTotal} (default {@link
 * BuildIndicesConfiguration#DEFAULT_PRE_ALIAS_SWAP_MAX_MCL_LAG_TOTAL}). If lag is too high or Kafka
 * admin fails, waits with exponential backoff (see {@link BuildIndicesConfiguration
 * #preAliasSwapLagRetryInitialBackoffMs} / {@code preAliasSwapLagRetryMaxBackoffMs}) and retries up
 * to {@code preAliasSwapLagStepRetries} additional times. {@link #retryCount()} is {@code 0} so
 * {@link com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager} does not add a second retry
 * layer. {@link BuildIndicesConfiguration#getPreAliasSwapLagRetryInitialBackoffMs()} and {@link
 * BuildIndicesConfiguration#getPreAliasSwapLagRetryMaxBackoffMs()} bound the wait between attempts.
 */
@Slf4j
public class IncrementalReindexPreAliasSwapLagStep implements UpgradeStep {

  static final String UPGRADE_ID_PREFIX = "IncrementalReindexPreAliasSwapLag";

  private static final int MAX_BACKOFF_SHIFT = 30;

  private final String upgradeVersion;
  private final long maxTotalLag;
  private final int maxAttempts;
  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final String consumerGroupId;
  private final String versionedTopicName;
  private final String timeseriesTopicName;
  private final KafkaConfiguration kafkaConfiguration;
  private final KafkaProperties kafkaProperties;

  public IncrementalReindexPreAliasSwapLagStep(
      String upgradeVersion,
      long maxTotalLag,
      int lagStepRetries,
      long initialBackoffMs,
      long maxBackoffMs,
      @Nonnull String mclConsumerGroupId,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties,
      Map<String, TopicsConfiguration.TopicConfiguration> kafkaTopics) {
    this.upgradeVersion = upgradeVersion;
    this.maxTotalLag = maxTotalLag;
    int retries = Math.max(0, lagStepRetries);
    this.maxAttempts = 1 + retries;
    this.initialBackoffMs = Math.max(0L, initialBackoffMs);
    this.maxBackoffMs = Math.max(this.initialBackoffMs, Math.max(0L, maxBackoffMs));
    this.consumerGroupId = mclConsumerGroupId;
    this.versionedTopicName = topicName(kafkaTopics, "metadataChangeLogVersioned");
    this.timeseriesTopicName = topicName(kafkaTopics, "metadataChangeLogTimeseries");
    this.kafkaConfiguration = kafkaConfiguration;
    this.kafkaProperties = kafkaProperties;
  }

  private static String topicName(
      Map<String, TopicsConfiguration.TopicConfiguration> topics, String key) {
    TopicsConfiguration.TopicConfiguration tc = topics.get(key);
    if (tc == null || tc.getName() == null) {
      throw new IllegalStateException("Missing Kafka topic configuration for key: " + key);
    }
    return tc.getName();
  }

  @Override
  public String id() {
    return UPGRADE_ID_PREFIX + "_" + upgradeVersion;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        if (attempt > 0) {
          long delayMs = backoffMsAfterFailures(attempt);
          if (!sleepQuietly(delayMs)) {
            log.warn("Pre-alias MCL lag step interrupted during backoff");
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
        }
        try (Admin admin =
            AdminClientFactory.buildKafkaAdminClient(
                kafkaConfiguration, kafkaProperties, "datahub-upgrade-pre-alias-lag")) {
          long lag =
              MclConsumerLagUtils.combinedMedianLag(
                  admin, consumerGroupId, versionedTopicName, timeseriesTopicName);
          log.info(
              "Pre-alias MCL lag check (attempt {}/{}): totalMedianLag={} threshold={} groupId={}",
              attempt + 1,
              maxAttempts,
              lag,
              maxTotalLag,
              consumerGroupId);
          if (lag <= maxTotalLag) {
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
          }
          log.warn(
              "MCL lag {} exceeds configured maximum {} (attempt {}/{})",
              lag,
              maxTotalLag,
              attempt + 1,
              maxAttempts);
          if (attempt == maxAttempts - 1) {
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
        } catch (Exception e) {
          log.error("Pre-alias MCL lag check failed (attempt {}/{})", attempt + 1, maxAttempts, e);
          if (attempt == maxAttempts - 1) {
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
        }
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    };
  }

  /** After {@code failureCount} failed attempts, wait this long before the next attempt. */
  long backoffMsAfterFailures(int failureCount) {
    if (failureCount <= 0) {
      return 0L;
    }
    int exp = Math.min(MAX_BACKOFF_SHIFT, failureCount - 1);
    return Math.min(maxBackoffMs, initialBackoffMs * (1L << exp));
  }

  private static boolean sleepQuietly(long delayMs) {
    if (delayMs <= 0) {
      return true;
    }
    try {
      Thread.sleep(delayMs);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  @Override
  public int retryCount() {
    return 0;
  }
}
