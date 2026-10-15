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
    // A unique cluster name per member is the real isolation guarantee: two members only share
    // state if they join the same cluster, and Hazelcast refuses to join across differing cluster
    // names. Without this, parallel Gradle test forks on one CI host can discover each other and
    // form a cluster — then the fleet-wide global map (a fixed name) is shared across unrelated
    // test classes, and buckets cross-contaminate (flaky over- and under-counting).
    config.setClusterName(unique);
    JoinConfig join = config.getNetworkConfig().getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(false);
    // Auto-detection is ON by default in Hazelcast 5; it can discover peers on the same host even
    // with multicast/TCP-IP off. Disable it so each member is truly standalone.
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
