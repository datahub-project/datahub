package com.linkedin.metadata;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;

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

    private static final int HTTP_PORT = 9200;

    // A helper method to create an ElasticseachContainer defaulting to the current image and version, with the ability
    // within firewalled environments to override with an environment variable to point to the offline repository.
    @Nonnull
    public static final ElasticsearchContainer getNewElasticsearchContainer() {
        return new ElasticsearchContainer(DOCKER_IMAGE_NAME);
    }

    // A helper method to construct a standard rest client for Elastic search.
    @Nonnull
    public static RestHighLevelClient buildRestClient(ElasticsearchContainer elasticsearchContainer) {
        final RestClientBuilder builder =
                RestClient.builder(new HttpHost("localhost", elasticsearchContainer.getMappedPort(HTTP_PORT), "http"))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
                                IOReactorConfig.custom().setIoThreadCount(1).build()));

        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
                setConnectionRequestTimeout(3000));

        return new RestHighLevelClient(builder);
    }

}
