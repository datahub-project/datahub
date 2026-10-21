package com.linkedin.metadata.ratelimit;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.UUID;

final class HazelcastTestSupport {

  private HazelcastTestSupport() {}

  static HazelcastInstance createIsolatedInstance() {
    String unique = "rate-limit-test-" + UUID.randomUUID();
    Config config = new Config();
    config.setInstanceName(unique);
    // Members share state only inside one cluster. Instance name does not do that.
    // Parallel Gradle forks (maxParallelForks = ncpu/2) join the default cluster "dev"
    // unless this is unique. Auto-detection is on by default in Hazelcast 5 and can
    // find peers on the same host with multicast and TCP/IP already off.
    config.setClusterName(unique);
    JoinConfig join = config.getNetworkConfig().getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(false);
    join.getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  static void shutdown(HazelcastInstance instance) {
    if (instance != null) {
      instance.shutdown();
    }
  }
}
