package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import java.util.List;
import org.testng.annotations.Test;

public class PgQueueConsumerPartitionShardingTest {

  @Test
  public void effectiveConcurrency_cappedByPartitionCount() {
    assertEquals(PgQueueConsumerPartitionSharding.effectiveConcurrency(8, 3), 3);
    assertEquals(PgQueueConsumerPartitionSharding.effectiveConcurrency(2, 8), 2);
  }

  @Test
  public void partitionsForWorker_modAssignment() {
    List<Integer> t0 = PgQueueConsumerPartitionSharding.partitionsForWorker(8, 3, 0);
    assertEquals(t0, List.of(0, 3, 6));
    assertEquals(PgQueueConsumerPartitionSharding.partitionsForWorker(8, 3, 1), List.of(1, 4, 7));
    assertEquals(PgQueueConsumerPartitionSharding.partitionsForWorker(8, 3, 2), List.of(2, 5));
    assertTrue(PgQueueConsumerPartitionSharding.partitionsForWorker(8, 3, 3).isEmpty());
  }

  @Test
  public void workerShardCount_maxAcrossTopics() {
    PgQueueResolvedTopicCatalogEntry low =
        new PgQueueResolvedTopicCatalogEntry("a", "TopicA", 4, "1", 0, 0L, 0L, false, 2);
    PgQueueResolvedTopicCatalogEntry high =
        new PgQueueResolvedTopicCatalogEntry("b", "TopicB", 8, "1", 0, 0L, 0L, false, 4);
    PgQueueSetupOptions opts =
        new PgQueueSetupOptions(
            "q",
            "p",
            2,
            60,
            "1",
            0,
            0L,
            0L,
            "application/avro",
            "1 day",
            4,
            false,
            3600,
            5000,
            false,
            1,
            List.of(low, high));
    assertEquals(
        PgQueueConsumerPartitionSharding.workerShardCount(List.of("TopicA", "TopicB"), opts), 4);
    assertEquals(PgQueueConsumerPartitionSharding.workerShardCount(List.of("TopicA"), opts), 2);
  }

  @Test
  public void configuredConcurrencyForTopic_usesCatalogThenDefault() {
    PgQueueResolvedTopicCatalogEntry t =
        new PgQueueResolvedTopicCatalogEntry("k", "Named", 2, "1", 0, 0L, 0L, false, 3);
    PgQueueSetupOptions opts =
        new PgQueueSetupOptions(
            "q",
            "p",
            2,
            60,
            "1",
            0,
            0L,
            0L,
            "application/avro",
            "1 day",
            4,
            false,
            3600,
            5000,
            false,
            5,
            List.of(t));
    assertEquals(PgQueueConsumerPartitionSharding.configuredConcurrencyForTopic(opts, "Named"), 2);
    assertEquals(PgQueueConsumerPartitionSharding.configuredConcurrencyForTopic(opts, "Other"), 2);
  }
}
