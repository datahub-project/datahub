package io.datahubproject.test.search;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class ElasticsearchTestContainer implements SearchTestContainer {
  private final String elasticVersion = System.getProperty("ELASTIC_VERSION");
  private final String elasticImageName =
      System.getProperty("ELASTIC_IMAGE_NAME", "docker.elastic.co/elasticsearch/elasticsearch");

  protected final GenericContainer<?> esContainer;
  private boolean isStarted = false;

  public ElasticsearchTestContainer() {
    String elasticImageFullName = elasticImageName + ":" + elasticVersion;

    // A helper method to create an ElasticsearchContainer defaulting to the current image and
    // version, with the ability
    // within firewalled environments to override with an environment variable to point to the
    // offline
    // repository.
    DockerImageName dockerImageName =
        DockerImageName.parse(elasticImageFullName).asCompatibleSubstituteFor(elasticImageName);
    this.esContainer =
        new org.testcontainers.elasticsearch.ElasticsearchContainer(dockerImageName)
            .withEnv(
                "xpack.security.enabled",
                "false"); // ES8+ enables this property, but we don't need SSL in tests
    checkContainerEngine(esContainer.getDockerClient());
    esContainer.withEnv("ES_JAVA_OPTS", SEARCH_JAVA_OPTS).withStartupTimeout(STARTUP_TIMEOUT);
  }

  @Override
  public GenericContainer<?> startContainer() {
    if (!isStarted) {
      esContainer.start();
      isStarted = true;
    }
    return esContainer;
  }

  @Override
  public void stopContainer() {
    esContainer.stop();
  }
}
