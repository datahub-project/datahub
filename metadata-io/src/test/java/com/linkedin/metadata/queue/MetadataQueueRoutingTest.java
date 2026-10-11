package com.linkedin.metadata.queue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MetadataQueueRoutingTest {

  @Test
  public void stablePartitionId_matchesPythonCrc32() {
    // Python: zlib.crc32(b"hello") & 0xFFFFFFFF == 907060870
    Assert.assertEquals(MetadataQueueRouting.stablePartitionId("hello", 4), 2);
    Assert.assertEquals(MetadataQueueRouting.stablePartitionId("hello", 10), 0);
  }

  @Test
  public void stablePartitionId_range() {
    for (int pc : new int[] {1, 2, 17, 256}) {
      int p = MetadataQueueRouting.stablePartitionId("rk", pc);
      Assert.assertTrue(p >= 0 && p < pc);
    }
  }

  @Test
  public void advisoryLockKey_deterministic() {
    long a = MetadataQueueRouting.advisoryLockKey(1L, 0);
    long b = MetadataQueueRouting.advisoryLockKey(1L, 0);
    Assert.assertEquals(a, b);
    Assert.assertTrue(a >= 0L);
  }
}
