package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import java.util.Optional;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.Test;

// Class-level @Test so Gradle's test class scan picks up the inherited test methods.
@Test
public class LegacyUsageEventIndexMigrationOpenSearchTest
    extends LegacyUsageEventIndexMigrationTestBase {

  // Same override metadata-io's OpenSearch test container honors.
  private static final String IMAGE =
      Optional.ofNullable(System.getenv("OPENSEARCH_IMAGE_FULL_NAME"))
          .orElse("opensearchproject/opensearch:2.19.3");

  @Override
  protected GenericContainer<?> createContainer() {
    return new GenericContainer<>(DockerImageName.parse(IMAGE))
        .withExposedPorts(9200)
        .withEnv("discovery.type", "single-node")
        .withEnv("DISABLE_SECURITY_PLUGIN", "true")
        .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .waitingFor(Wait.forHttp("/").forPort(9200).forStatusCode(200))
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  @Override
  protected String managedLayoutField() {
    return "aliases";
  }
}
