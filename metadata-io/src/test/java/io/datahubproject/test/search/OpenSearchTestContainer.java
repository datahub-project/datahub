package io.datahubproject.test.search;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;

import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class OpenSearchTestContainer implements SearchTestContainer {
  private static final String OPENSEARCH_VERSION = "2.11.0";
  private static final String OPENSEARCH_IMAGE_NAME = "opensearchproject/opensearch";
  private static final String ENV_OPENSEARCH_IMAGE_FULL_NAME =
      System.getenv("OPENSEARCH_IMAGE_FULL_NAME");
  private static final String OPENSEARCH_IMAGE_FULL_NAME =
      ENV_OPENSEARCH_IMAGE_FULL_NAME != null
          ? ENV_OPENSEARCH_IMAGE_FULL_NAME
          : OPENSEARCH_IMAGE_NAME + ":" + OPENSEARCH_VERSION;
  private static final DockerImageName DOCKER_IMAGE_NAME =
      DockerImageName.parse(OPENSEARCH_IMAGE_FULL_NAME)
          .asCompatibleSubstituteFor(OPENSEARCH_IMAGE_NAME);

  protected static final GenericContainer<?> OS_CONTAINER;
  private boolean isStarted = false;

  // A helper method to create an ElasticseachContainer defaulting to the current image and version,
  // with the ability
  // within firewalled environments to override with an environment variable to point to the offline
  // repository.
  static {
    OS_CONTAINER = new OpensearchContainer(DOCKER_IMAGE_NAME);
    checkContainerEngine(OS_CONTAINER.getDockerClient());
    OS_CONTAINER
        .withEnv("OPENSEARCH_JAVA_OPTS", SEARCH_JAVA_OPTS)
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  @Override
  public GenericContainer<?> startContainer() {
    if (!isStarted) {
      OS_CONTAINER.start();
      isStarted = true;
    }
    return OS_CONTAINER;
  }

  @Override
  public void stopContainer() {
    OS_CONTAINER.stop();
  }
}
