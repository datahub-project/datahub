package com.datahub.auth.authentication;

import com.datahub.authentication.token.TokenRevocationMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

final class AuthFilterHazelcast {

  private static final HazelcastInstance INSTANCE = start();

  private AuthFilterHazelcast() {}

  static HazelcastInstance instance() {
    return INSTANCE;
  }

  private static HazelcastInstance start() {
    Config config = new Config();
    config.setClusterName("auth-filter-token-tests");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.addMapConfig(TokenRevocationMap.mapConfig());
    return Hazelcast.newHazelcastInstance(config);
  }
}
