package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.Test;

// Class-level @Test so Gradle's test class scan picks up the inherited test methods.
@Test
public class LegacyUsageEventIndexMigrationElasticsearchTest
    extends LegacyUsageEventIndexMigrationTestBase {

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

  @Override
  protected GenericContainer<?> createContainer() {
    return new ElasticsearchContainer(
            DockerImageName.parse(IMAGE + ":8.17.4").asCompatibleSubstituteFor(IMAGE))
        .withEnv("xpack.security.enabled", "false")
        .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  @Override
  protected String managedLayoutField() {
    return "data_streams";
  }
}
