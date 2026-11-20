package com.linkedin.metadata.ratelimit;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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

  /**
   * Resets all bucket state so one Hazelcast member can be shared across a class's test methods:
   * booting a member costs seconds, but recreating its maps is cheap. Bucket4j stores each bucket
   * as a map entry, so tearing down every {@link IMap} (tenant + shared global) means the next test
   * (which builds a fresh store and re-gets the maps) sees every bucket at full capacity — the same
   * clean slate a fresh member gives, without the startup cost. Call from {@code @AfterMethod}.
   *
   * <p>Uses {@code destroy()} rather than {@code clear()} deliberately: {@code destroy()} removes
   * the distributed object wholesale (a subsequent {@code getMap(name)} returns a brand-new
   * structure), whereas {@code clear()} only empties entries and left rare cross-method state bleed
   * under CI load — a consume that should have been denied was occasionally admitted.
   */
  static void resetAllMaps(HazelcastInstance instance) {
    if (instance == null) {
      return;
    }
    for (DistributedObject object : instance.getDistributedObjects()) {
      if (object instanceof IMap<?, ?> map) {
        map.destroy();
      }
    }
  }
}
