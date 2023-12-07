package io.datahubproject.test.search;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.AUTO_COMPLETE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;

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
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;

public class SearchTestUtils {
  private SearchTestUtils() {}

  public static void syncAfterWrite(ESBulkProcessor bulkProcessor) throws InterruptedException {
    bulkProcessor.flush();
    Thread.sleep(1000);
  }

  public static final List<String> SEARCHABLE_ENTITIES;

  static {
    SEARCHABLE_ENTITIES =
        Stream.concat(SEARCHABLE_ENTITY_TYPES.stream(), AUTO_COMPLETE_ENTITY_TYPES.stream())
            .map(EntityTypeMapper::getName)
            .distinct()
            .collect(Collectors.toList());
  }

  public static SearchResult searchAcrossEntities(SearchService searchService, String query) {
    return searchAcrossEntities(searchService, query, null);
  }

  public static SearchResult searchAcrossEntities(
      SearchService searchService, String query, @Nullable List<String> facets) {
    return searchService.searchAcrossEntities(
        SEARCHABLE_ENTITIES,
        query,
        null,
        null,
        0,
        100,
        new SearchFlags().setFulltext(true).setSkipCache(true),
        facets);
  }

  public static SearchResult searchAcrossCustomEntities(
      SearchService searchService, String query, List<String> searchableEntities) {
    return searchService.searchAcrossEntities(
        searchableEntities,
        query,
        null,
        null,
        0,
        100,
        new SearchFlags().setFulltext(true).setSkipCache(true));
  }

  public static SearchResult search(SearchService searchService, String query) {
    return search(searchService, SEARCHABLE_ENTITIES, query);
  }

  public static SearchResult search(
      SearchService searchService, List<String> entities, String query) {
    return searchService.search(
        entities,
        query,
        null,
        null,
        0,
        100,
        new SearchFlags().setFulltext(true).setSkipCache(true));
  }

  public static ScrollResult scroll(
      SearchService searchService, String query, int batchSize, @Nullable String scrollId) {
    return searchService.scrollAcrossEntities(
        SEARCHABLE_ENTITIES,
        query,
        null,
        null,
        scrollId,
        "3m",
        batchSize,
        new SearchFlags().setFulltext(true).setSkipCache(true));
  }

  public static SearchResult searchStructured(SearchService searchService, String query) {
    return searchService.searchAcrossEntities(
        SEARCHABLE_ENTITIES,
        query,
        null,
        null,
        0,
        100,
        new SearchFlags().setFulltext(false).setSkipCache(true));
  }

  public static LineageSearchResult lineage(
      LineageSearchService lineageSearchService, Urn root, int hops) {
    String degree = hops >= 3 ? "3+" : String.valueOf(hops);
    List<FacetFilterInput> filters =
        List.of(
            FacetFilterInput.builder()
                .setField("degree")
                .setCondition(FilterOperator.EQUAL)
                .setValues(List.of(degree))
                .setNegated(false)
                .build());

    return lineageSearchService.searchAcrossLineage(
        root,
        LineageDirection.DOWNSTREAM,
        SEARCHABLE_ENTITY_TYPES.stream()
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toList()),
        "*",
        hops,
        ResolverUtils.buildFilter(filters, List.of()),
        null,
        0,
        100,
        null,
        null,
        new SearchFlags().setSkipCache(true));
  }

  public static AutoCompleteResults autocomplete(
      SearchableEntityType<?, String> searchableEntityType, String query) throws Exception {
    return searchableEntityType.autoComplete(
        query,
        null,
        null,
        100,
        new QueryContext() {
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
    Integer port =
        Integer.parseInt(Optional.ofNullable(System.getenv("ELASTICSEARCH_PORT")).orElse("9200"));
    return RestClient.builder(
            new HttpHost(
                Optional.ofNullable(System.getenv("ELASTICSEARCH_HOST")).orElse("localhost"),
                port,
                port.equals(443) ? "https" : "http"))
        .setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();

                if (System.getenv("ELASTICSEARCH_USERNAME") != null) {
                  final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                  credentialsProvider.setCredentials(
                      AuthScope.ANY,
                      new UsernamePasswordCredentials(
                          System.getenv("ELASTICSEARCH_USERNAME"),
                          System.getenv("ELASTICSEARCH_PASSWORD")));
                  httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }

                return httpClientBuilder;
              }
            });
  }
}
