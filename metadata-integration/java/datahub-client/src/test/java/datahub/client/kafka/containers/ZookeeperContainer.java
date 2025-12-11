/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.kafka.containers;

import static datahub.client.kafka.containers.Utils.CONFLUENT_PLATFORM_VERSION;
import static java.lang.String.format;

import java.io.IOException;
import java.util.HashMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.TestcontainersConfiguration;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

  private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
  private static final int ZOOKEEPER_TICK_TIME = 2000;

  private final String networkAlias = "zookeeper";

  public ZookeeperContainer() throws IOException {
    this(CONFLUENT_PLATFORM_VERSION);
  }

  public ZookeeperContainer(String confluentPlatformVersion) throws IOException {
    super(getZookeeperContainerImage(confluentPlatformVersion));

    HashMap<String, String> env = new HashMap<String, String>();
    env.put("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
    env.put("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));
    withEnv(env);

    addExposedPort(ZOOKEEPER_INTERNAL_PORT);
    withNetworkAliases(networkAlias);
    waitingFor(new HostPortWaitStrategy());
  }

  public String getInternalUrl() {
    return format("%s:%d", networkAlias, ZOOKEEPER_INTERNAL_PORT);
  }

  private static String getZookeeperContainerImage(String confluentPlatformVersion) {
    return (String)
        TestcontainersConfiguration.getInstance()
            .getProperties()
            .getOrDefault(
                "zookeeper.container.image",
                "confluentinc/cp-zookeeper:" + confluentPlatformVersion);
  }
}
