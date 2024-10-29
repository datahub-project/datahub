package io.datahubproject.test.search;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class ElasticsearchTestContainer implements SearchTestContainer {
  private static final String ELASTIC_VERSION = "7.10.1";
  private static final String ELASTIC_IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch";
  private static final String ENV_ELASTIC_IMAGE_FULL_NAME =
      System.getenv("ELASTIC_IMAGE_FULL_NAME");
  private static final String ELASTIC_IMAGE_FULL_NAME =
      ENV_ELASTIC_IMAGE_FULL_NAME != null
          ? ENV_ELASTIC_IMAGE_FULL_NAME
          : ELASTIC_IMAGE_NAME + ":" + ELASTIC_VERSION;
  private static final DockerImageName DOCKER_IMAGE_NAME =
      DockerImageName.parse(ELASTIC_IMAGE_FULL_NAME).asCompatibleSubstituteFor(ELASTIC_IMAGE_NAME);

  protected static final GenericContainer<?> ES_CONTAINER;
  private boolean isStarted = false;

  // A helper method to create an ElasticsearchContainer defaulting to the current image and
  // version, with the ability
  // within firewalled environments to override with an environment variable to point to the offline
  // repository.
  static {
    ES_CONTAINER = new org.testcontainers.elasticsearch.ElasticsearchContainer(DOCKER_IMAGE_NAME);
    checkContainerEngine(ES_CONTAINER.getDockerClient());
    ES_CONTAINER.withEnv("ES_JAVA_OPTS", SEARCH_JAVA_OPTS).withStartupTimeout(STARTUP_TIMEOUT);
  }

  @Override
  public GenericContainer<?> startContainer() {
    if (!isStarted) {
      ElasticsearchTestContainer.ES_CONTAINER.start();
      isStarted = true;
    }
    return ES_CONTAINER;
  }

  @Override
  public void stopContainer() {
    ES_CONTAINER.stop();
  }
}
