package datahub.client.kafka.containers;

import static datahub.client.kafka.containers.Utils.CONFLUENT_PLATFORM_VERSION;
import static java.lang.String.format;

import java.io.IOException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;


public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

    private final String networkAlias = "schema-registry";

    public SchemaRegistryContainer(String zookeeperConnect) throws IOException {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public SchemaRegistryContainer(String confluentPlatformVersion, String zookeeperConnect) throws IOException {
        super(getSchemaRegistryContainerImage(confluentPlatformVersion));

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperConnect);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases(networkAlias);

        waitingFor(Wait.forHttp("/subjects"));
    }

    public String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }


    private static String getSchemaRegistryContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "schemaregistry.container.image",
                        "confluentinc/cp-schema-registry:" + confluentPlatformVersion
                );
    }
}


