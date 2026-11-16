package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

import com.hazelcast.core.HazelcastInstance;
import org.testng.annotations.Test;

public class HazelcastTestSupportIsolationTest {

  @Test
  public void testTwoInstancesDoNotShareACluster() {
    HazelcastInstance first = HazelcastTestSupport.createIsolatedInstance();
    HazelcastInstance second = HazelcastTestSupport.createIsolatedInstance();
    try {
      assertNotEquals(first.getConfig().getClusterName(), second.getConfig().getClusterName());
      assertEquals(first.getCluster().getMembers().size(), 1);
      assertEquals(second.getCluster().getMembers().size(), 1);

      first.getMap("isolation").put("key", "value");
      assertNull(second.getMap("isolation").get("key"));
    } finally {
      HazelcastTestSupport.shutdown(second);
      HazelcastTestSupport.shutdown(first);
    }
  }
}
