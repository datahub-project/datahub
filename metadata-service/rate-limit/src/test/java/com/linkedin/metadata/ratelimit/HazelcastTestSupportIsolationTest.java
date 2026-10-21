package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import com.hazelcast.core.HazelcastInstance;
import org.testng.annotations.Test;

public class HazelcastTestSupportIsolationTest {

  @Test
  public void testTwoInstancesDoNotShareACluster() {
    HazelcastInstance first = HazelcastTestSupport.createIsolatedInstance();
    HazelcastInstance second = HazelcastTestSupport.createIsolatedInstance();
    try {
      assertNotEquals(first.getConfig().getClusterName(), second.getConfig().getClusterName());
      assertFalse(first.getConfig().getNetworkConfig().getJoin().isAutoDetectionEnabled());
      assertFalse(second.getConfig().getNetworkConfig().getJoin().isAutoDetectionEnabled());
      assertFalse(first.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled());
      assertFalse(second.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().isEnabled());
      assertEquals(first.getCluster().getMembers().size(), 1);
      assertEquals(second.getCluster().getMembers().size(), 1);
    } finally {
      HazelcastTestSupport.shutdown(second);
      HazelcastTestSupport.shutdown(first);
    }
  }
}
