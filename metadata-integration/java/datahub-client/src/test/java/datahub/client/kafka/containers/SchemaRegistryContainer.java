package datahub.client.kafka.containers;

import static datahub.client.kafka.containers.Utils.CONFLUENT_PLATFORM_VERSION;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

/** This container wraps Confluent Schema Registry. */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

  private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

  private final String networkAlias = "schema-registry";

  public SchemaRegistryContainer(String kafkaBootstrapServers) {
    this(CONFLUENT_PLATFORM_VERSION, kafkaBootstrapServers);
  }

  public SchemaRegistryContainer(String confluentPlatformVersion, String kafkaBootstrapServers) {
    super(getSchemaRegistryContainerImage(confluentPlatformVersion));

    withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
    withNetworkAliases(networkAlias);

    withEnv("SCHEMA_REGISTRY_HOST_NAME", networkAlias);
    withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_INTERNAL_PORT);
    withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaBootstrapServers);

    waitingFor(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_INTERNAL_PORT));
  }

  public String getUrl() {
    return String.format("http://%s:%d", getHost(), getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
  }

  private static String getSchemaRegistryContainerImage(String confluentPlatformVersion) {
    return (String)
        TestcontainersConfiguration.getInstance()
            .getProperties()
            .getOrDefault(
                "schemaregistry.container.image",
                "confluentinc/cp-schema-registry:" + confluentPlatformVersion);
  }
}
