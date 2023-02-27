package com.linkedin.metadata;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.DockerTestUtils.*;

public class ESTestUtils {
    private ESTestUtils() {
    }

    private static final String ELASTIC_VERSION = "7.10.1";
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
        ES_CONTAINER.withEnv("ES_JAVA_OPTS", "-Xms64m -Xmx384m -XX:MaxDirectMemorySize=368435456")
                .withStartupTimeout(Duration.ofMinutes(5)); // usually < 1min
    }

    public final static List<String> SEARCHABLE_ENTITIES;
    static {
        SEARCHABLE_ENTITIES = Stream.concat(SEARCHABLE_ENTITY_TYPES.stream(), AUTO_COMPLETE_ENTITY_TYPES.stream())
                .map(EntityTypeMapper::getName)
                .distinct()
                .collect(Collectors.toList());
    }

    public static SearchResult search(SearchService searchService, String query) {
        return searchService.searchAcrossEntities(SEARCHABLE_ENTITIES, query, null, null, 0,
                100, new SearchFlags().setFulltext(true));
    }

    public static SearchResult searchStructured(SearchService searchService, String query) {
        return searchService.searchAcrossEntities(SEARCHABLE_ENTITIES, query, null, null, 0,
                100, new SearchFlags().setFulltext(false));
    }

    public static LineageSearchResult lineage(LineageSearchService lineageSearchService, Urn root, int hops) {
        String degree = hops >= 3 ? "3+" : String.valueOf(hops);
        List<FacetFilterInput> filters = List.of(FacetFilterInput.builder()
                .setField("degree")
                .setCondition(FilterOperator.EQUAL)
                .setValues(List.of(degree))
                .setNegated(false)
                .build());

        return lineageSearchService.searchAcrossLineage(root, LineageDirection.DOWNSTREAM,
            SEARCHABLE_ENTITY_TYPES.stream().map(EntityTypeMapper::getName).collect(Collectors.toList()),
            "*", hops, ResolverUtils.buildFilter(filters, List.of()), null, 0, 100, null,
            null, new SearchFlags().setSkipCache(true));
    }

    public static AutoCompleteResults autocomplete(SearchableEntityType<?, String> searchableEntityType, String query) throws Exception {
        return searchableEntityType.autoComplete(query, null, null, 100, new QueryContext() {
            @Override
            public boolean isAuthenticated() {
                return true;
            }

            @Override
            public Authentication getAuthentication() {
                return null;
            }

            @Override
            public Authorizer getAuthorizer() {
                return null;
            }
        });
    }

    public static RestClientBuilder environmentRestClientBuilder() {
        Integer port = Integer.parseInt(Optional.ofNullable(System.getenv("ELASTICSEARCH_PORT")).orElse("9200"));
        return RestClient.builder(
                        new HttpHost(Optional.ofNullable(System.getenv("ELASTICSEARCH_HOST")).orElse("localhost"),
                                port, port.equals(443) ? "https" : "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.disableAuthCaching();

                        if (System.getenv("ELASTICSEARCH_USERNAME") != null) {
                            final CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                    new UsernamePasswordCredentials(System.getenv("ELASTICSEARCH_USERNAME"),
                                            System.getenv("ELASTICSEARCH_PASSWORD")));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }

                        return httpClientBuilder;
                    }
                });
    }
}