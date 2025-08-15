package datahub.client.kafka.containers;

import static datahub.client.kafka.containers.Utils.CONFLUENT_PLATFORM_VERSION;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

/** This container wraps Confluent Kafka using KRaft mode (no Zookeeper required). */
public class KafkaContainer extends GenericContainer<KafkaContainer> {

  private static final int KAFKA_INTERNAL_PORT = 9092;
  private static final int KAFKA_EXTERNAL_PORT = 9093;
  private static final int KAFKA_CONTROLLER_PORT = 29093;
  public static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29092;

  private final String networkAlias = "kafka";

  public KafkaContainer() {
    this(CONFLUENT_PLATFORM_VERSION);
  }

  public KafkaContainer(String confluentPlatformVersion) {
    super(getKafkaContainerImage(confluentPlatformVersion));

    // Fix the external port to 9093 to match the advertised listener
    // Only expose the external port, not the internal port to avoid conflicts
    addFixedExposedPort(KAFKA_EXTERNAL_PORT, KAFKA_EXTERNAL_PORT);

    // KRaft configuration
    String clusterId = java.util.UUID.randomUUID().toString();
    withEnv("KAFKA_NODE_ID", "1");
    withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
    withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + networkAlias + ":" + KAFKA_CONTROLLER_PORT);
    withEnv("CLUSTER_ID", clusterId);
    withEnv("KAFKA_CLUSTER_ID", clusterId);

    // Configure listeners
    withEnv(
        "KAFKA_LISTENERS",
        "PLAINTEXT://0.0.0.0:"
            + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
            + ",CONTROLLER://0.0.0.0:"
            + KAFKA_CONTROLLER_PORT
            + ",BROKER://0.0.0.0:"
            + KAFKA_INTERNAL_PORT
            + ",BROKER_EXTERNAL://0.0.0.0:"
            + KAFKA_EXTERNAL_PORT);

    withEnv(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,BROKER_EXTERNAL:PLAINTEXT");

    // Advertised listeners - external port must match the container port
    withEnv(
        "KAFKA_ADVERTISED_LISTENERS",
        String.format(
            "PLAINTEXT://%s:%d,BROKER://%s:%d,BROKER_EXTERNAL://localhost:%d",
            networkAlias,
            KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT,
            networkAlias,
            KAFKA_INTERNAL_PORT,
            KAFKA_EXTERNAL_PORT // This must be 9093, not a mapped port
            ));

    withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
    withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    // Common Kafka settings
    withEnv("KAFKA_BROKER_ID", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    withNetworkAliases(networkAlias);

    // Wait for Kafka to be ready by checking the external port
    waitingFor(Wait.forListeningPort());
  }

  public String getBootstrapServers() {
    return "localhost:" + KAFKA_EXTERNAL_PORT;
  }

  public String getInternalBootstrapServers() {
    return networkAlias + ":" + KAFKA_INTERNAL_PORT;
  }

  private static String getKafkaContainerImage(String confluentPlatformVersion) {
    return (String)
        TestcontainersConfiguration.getInstance()
            .getProperties()
            .getOrDefault(
                "kafka.container.image", "confluentinc/cp-kafka:" + confluentPlatformVersion);
  }
}
