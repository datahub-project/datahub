package com.linkedin.metadata.usage;

import com.linkedin.metadata.usage.flush.DistinctIdentitySet;
import io.datahubproject.metadata.context.usage.AttributionType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DistinctIdentitySetTest {

  @Test
  public void testAddReturnsNewOnlyOncePerUsageIdentity() {
    DistinctIdentitySet set = new DistinctIdentitySet();

    Assert.assertTrue(set.add("urn:li:corpuser:a", AttributionType.HUMAN));
    Assert.assertFalse(set.add("urn:li:corpuser:a", AttributionType.AUTOMATED));
    Assert.assertTrue(set.add("urn:li:corpuser:b", AttributionType.HUMAN));
    Assert.assertEquals(set.size(), 2);
  }

  @Test
  public void testToEntriesPreservesUniqueIdentities() {
    DistinctIdentitySet set = new DistinctIdentitySet();
    set.add("urn:li:corpuser:a", AttributionType.HUMAN);
    set.add("urn:li:corpuser:b", AttributionType.HUMAN);

    Assert.assertEquals(
        set.toEntries().stream().map(entry -> entry.usageIdentity()).sorted().toList(),
        java.util.List.of("urn:li:corpuser:a", "urn:li:corpuser:b"));
  }

  @Test
  public void testMergeFromRestoresIdentitySet() {
    DistinctIdentitySet set = new DistinctIdentitySet();
    set.mergeFrom(
        java.util.List.of(
            new com.linkedin.metadata.usage.flush.DistinctIdentityEntry(
                "urn:li:corpuser:a", AttributionType.HUMAN),
            new com.linkedin.metadata.usage.flush.DistinctIdentityEntry(
                "urn:li:corpuser:b", AttributionType.HUMAN)));

    Assert.assertEquals(set.size(), 2);
  }

  @Test
  public void testConcurrentAddIsThreadSafe() throws Exception {
    DistinctIdentitySet set = new DistinctIdentitySet();
    int threadCount = 8;
    int addsPerThread = 50;
    java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threadCount);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(threadCount);
    try {
      for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        executor.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < addsPerThread; i++) {
                  set.add("urn:li:corpuser:" + threadId + "-" + i, AttributionType.HUMAN);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      Assert.assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
    Assert.assertEquals(set.size(), threadCount * addsPerThread);
  }
}
