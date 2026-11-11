package com.linkedin.metadata.config.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

/** Unit tests for the memoized {@link KafkaConfiguration#getTopics()}. */
public class KafkaConfigurationTopicsCacheTest {

  private static final int THREADS = 16;
  private static final int CALLS_PER_THREAD = 2_000;
  private static final int TOPIC_COUNT = 50;

  private static TopicsConfiguration.TopicConfiguration topicDefaults(int partitions) {
    TopicsConfiguration.TopicConfiguration defaults = new TopicsConfiguration.TopicConfiguration();
    defaults.setPartitions(partitions);
    defaults.setReplicationFactor(1);
    defaults.setConfigProperties(Map.of("max.message.bytes", "5242880"));
    return defaults;
  }

  private static Map<String, TopicsConfiguration.TopicConfiguration> topics(String prefix) {
    Map<String, TopicsConfiguration.TopicConfiguration> topics = new HashMap<>();
    for (int i = 0; i < TOPIC_COUNT; i++) {
      TopicsConfiguration.TopicConfiguration topic = new TopicsConfiguration.TopicConfiguration();
      topic.setName(prefix + "_" + i);
      // Own configProperties so the constructor takes the merge-and-replace path
      topic.setConfigProperties(Map.of("retention.ms", String.valueOf(1000L + i)));
      topics.put(prefix + i, topic);
    }
    return topics;
  }

  private static KafkaConfiguration kafkaConfiguration() {
    KafkaConfiguration config = new KafkaConfiguration();
    config.setTopicDefaults(topicDefaults(3));
    config.setTopics(topics("topic"));
    return config;
  }

  @Test
  public void testConcurrentGetTopicsReturnsSingleInstance() throws Exception {
    KafkaConfiguration config = kafkaConfiguration();
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    CountDownLatch start = new CountDownLatch(1);
    Set<TopicsConfiguration> seen =
        Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < CALLS_PER_THREAD; i++) {
                    TopicsConfiguration topicsConfiguration = config.getTopics();
                    seen.add(topicsConfiguration);
                    // Read the merged topics the way pgQueue does on every poll iteration
                    for (TopicsConfiguration.TopicConfiguration topic :
                        topicsConfiguration.getTopics().values()) {
                      for (Map.Entry<String, String> e : topic.getConfigProperties().entrySet()) {
                        e.getValue();
                      }
                    }
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        // Rethrows any exception raised in a worker thread
        future.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertEquals(seen.size(), 1, "All concurrent callers should get the same instance");
    assertSame(seen.iterator().next(), config.getTopics());
  }

  @Test
  public void testSetTopicsInvalidatesCache() {
    KafkaConfiguration config = kafkaConfiguration();
    TopicsConfiguration before = config.getTopics();
    assertSame(config.getTopics(), before);

    config.setTopics(topics("other"));
    TopicsConfiguration after = config.getTopics();

    assertNotSame(after, before);
    assertTrue(after.getTopics().containsKey("other0"));
    assertTrue(!after.getTopics().containsKey("topic0"));
    assertEquals(after.getTopics().get("other0").getPartitions(), Integer.valueOf(3));
    assertSame(config.getTopics(), after);
  }

  @Test
  public void testSetTopicDefaultsInvalidatesCache() {
    KafkaConfiguration config = kafkaConfiguration();
    TopicsConfiguration before = config.getTopics();

    // Topics whose partitions were already filled from the old defaults keep them, so use a
    // fresh topics map to observe the new defaults being applied.
    config.setTopicDefaults(topicDefaults(7));
    TopicsConfiguration afterDefaults = config.getTopics();
    assertNotSame(afterDefaults, before);
    assertEquals(afterDefaults.getTopicDefaults().getPartitions(), Integer.valueOf(7));

    config.setTopics(topics("fresh"));
    assertEquals(config.getTopics().getTopics().get("fresh0").getPartitions(), Integer.valueOf(7));
  }
}
