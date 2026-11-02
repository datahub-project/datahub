package com.datahub.authentication.token;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * One-member Hazelcast for token-service unit tests that are not checking cross-member
 * invalidation.
 */
public final class TestHazelcast {

  private static final HazelcastInstance INSTANCE = start("stateful-token-service-tests");

  private TestHazelcast() {}

  public static HazelcastInstance instance() {
    return INSTANCE;
  }

  static HazelcastInstance start(String clusterName) {
    Config config = new Config();
    config.setClusterName(clusterName);
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.addMapConfig(TokenRevocationMap.mapConfig());
    return Hazelcast.newHazelcastInstance(config);
  }
}
