package com.linkedin.metadata;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;

public class ElasticTestUtils {
    private ElasticTestUtils() {
    }

    private static final String ELASTIC_VERSION = "7.9.3";
    private static final String ELASTIC_IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch";
    private static final String ENV_ELASTIC_IMAGE_FULL_NAME = System.getenv("ELASTIC_IMAGE_FULL_NAME");
    private static final String ELASTIC_IMAGE_FULL_NAME = ENV_ELASTIC_IMAGE_FULL_NAME != null
            ? ENV_ELASTIC_IMAGE_FULL_NAME : ELASTIC_IMAGE_NAME + ":" + ELASTIC_VERSION;
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse(ELASTIC_IMAGE_FULL_NAME)
            .asCompatibleSubstituteFor(ELASTIC_IMAGE_NAME);

    public static final ElasticsearchContainer ES_CONTAINER;

    // A helper method to create an ElasticseachContainer defaulting to the current image and version, with the ability
    // within firewalled environments to override with an environment variable to point to the offline repository.
    static  {
        ES_CONTAINER = new ElasticsearchContainer(DOCKER_IMAGE_NAME);
        checkContainerEngine(ES_CONTAINER.getDockerClient());
        ES_CONTAINER.withEnv("ES_JAVA_OPTS", "-Xms64m -Xmx128m -XX:MaxDirectMemorySize=134217728")
                .withStartupTimeout(Duration.ofMinutes(5)); // usually < 1min
    }
}