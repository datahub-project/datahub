package datahub.client.kafka.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static datahub.client.kafka.containers.Utils.CONFLUENT_PLATFORM_VERSION;

/**
 * This container wraps Confluent Kafka.
 *
 */
public class KafkaContainer extends GenericContainer<KafkaContainer> {

  private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

  private static final int KAFKA_INTERNAL_PORT = 9092;
  private static final int KAFKA_LOCAL_PORT = 9093;

  public static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29092;

  private static final int PORT_NOT_ASSIGNED = -1;

  private String zookeeperConnect = null;

  private int port = PORT_NOT_ASSIGNED;

  private final String networkAlias = "kafka";

  public KafkaContainer(String zookeeperConnect) {
    this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
  }

  public KafkaContainer(String confluentPlatformVersion, String zookeeperConnect) {
    super(getKafkaContainerImage(confluentPlatformVersion));

    this.zookeeperConnect = zookeeperConnect;
    withExposedPorts(KAFKA_INTERNAL_PORT, KAFKA_LOCAL_PORT);

    // Use two listeners with different names, it will force Kafka to communicate
    // with itself via internal
    // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will
    // try to use the advertised listener
    withEnv("KAFKA_LISTENERS",
        "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
                + ",BROKER://0.0.0.0:" + KAFKA_INTERNAL_PORT
                + ",BROKER_LOCAL://0.0.0.0:" + KAFKA_LOCAL_PORT);
    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER_LOCAL:PLAINTEXT");
    withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    withEnv("KAFKA_BROKER_ID", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

    withNetworkAliases(networkAlias);
    waitingFor(new HostPortWaitStrategy());
  }

  public Stream<String> getBootstrapServers() {
    if (port == PORT_NOT_ASSIGNED) {
      throw new IllegalStateException("You should start Kafka container first");
    }
    return Stream.of(String.format("PLAINTEXT://%s:%s", getHost(), port),
            String.format("PLAINTEXT://localhost:%s", getMappedPort(KAFKA_LOCAL_PORT)));
  }

  @Override
  protected void doStart() {
    withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);

    super.doStart();
  }

  @Override
  protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
    super.containerIsStarting(containerInfo, reused);

    port = getMappedPort(KAFKA_INTERNAL_PORT);

    if (reused) {
      return;
    }

    // Use two listeners with different names, it will force Kafka to communicate
    // with itself via internal
    // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will
    // try to use the advertised listener

    String command = "#!/bin/bash \n";
    command += "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n";
    command += "export KAFKA_ADVERTISED_LISTENERS='" + Stream
        .concat(Stream.of("PLAINTEXT://" + networkAlias + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT,
                        "BROKER_LOCAL://localhost:" + getMappedPort(KAFKA_LOCAL_PORT)),
            containerInfo.getNetworkSettings().getNetworks().values().stream()
                .map(it -> "BROKER://" + it.getIpAddress() + ":" + KAFKA_INTERNAL_PORT))
        .collect(Collectors.joining(",")) + "'\n";

    command += ". /etc/confluent/docker/bash-config \n";
    command += "/etc/confluent/docker/configure \n";
    command += "/etc/confluent/docker/launch \n";

    copyFileToContainer(Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700), STARTER_SCRIPT);
  }

  private static String getKafkaContainerImage(String confluentPlatformVersion) {
    return (String) TestcontainersConfiguration.getInstance().getProperties().getOrDefault("kafka.container.image",
        "confluentinc/cp-kafka:" + confluentPlatformVersion);
  }
}