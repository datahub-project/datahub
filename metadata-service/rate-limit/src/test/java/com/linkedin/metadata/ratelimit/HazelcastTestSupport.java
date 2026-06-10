package com.linkedin.metadata.ratelimit;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.UUID;

final class HazelcastTestSupport {

  private HazelcastTestSupport() {}

  static HazelcastInstance createIsolatedInstance() {
    Config config = new Config();
    config.setInstanceName("rate-limit-test-" + UUID.randomUUID());
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  static void shutdown(HazelcastInstance instance) {
    if (instance != null) {
      instance.shutdown();
    }
  }
}
