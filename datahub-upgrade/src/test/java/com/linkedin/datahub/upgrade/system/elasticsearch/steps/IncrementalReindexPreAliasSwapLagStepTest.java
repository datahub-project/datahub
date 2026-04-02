package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.Test;

public class IncrementalReindexPreAliasSwapLagStepTest {

  private static IncrementalReindexPreAliasSwapLagStep newStep(
      long initialBackoffMs, long maxBackoffMs) {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    TopicsConfiguration.TopicConfiguration versioned = new TopicsConfiguration.TopicConfiguration();
    versioned.setName("MetadataChangeLog_Versioned_v1");
    topics.put("metadataChangeLogVersioned", versioned);
    TopicsConfiguration.TopicConfiguration timeseries =
        new TopicsConfiguration.TopicConfiguration();
    timeseries.setName("MetadataChangeLog_Timeseries_v1");
    topics.put("metadataChangeLogTimeseries", timeseries);
    return new IncrementalReindexPreAliasSwapLagStep(
        "0.1.0-0",
        5000L,
        5,
        initialBackoffMs,
        maxBackoffMs,
        "generic-mae-consumer-job-client",
        new KafkaConfiguration(),
        new KafkaProperties(),
        topics);
  }

  @Test
  public void backoffMsAfterFailures_exponentialUntilCap() {
    IncrementalReindexPreAliasSwapLagStep step = newStep(5000L, 60_000L);
    assertEquals(step.backoffMsAfterFailures(0), 0L);
    assertEquals(step.backoffMsAfterFailures(1), 5000L);
    assertEquals(step.backoffMsAfterFailures(2), 10_000L);
    assertEquals(step.backoffMsAfterFailures(3), 20_000L);
    assertEquals(step.backoffMsAfterFailures(10), 60_000L);
  }

  @Test
  public void retryCount_isZero() {
    assertEquals(newStep(5000L, 60_000L).retryCount(), 0);
  }
}
