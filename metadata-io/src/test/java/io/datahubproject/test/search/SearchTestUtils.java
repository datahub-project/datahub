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
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
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

  public static void syncAfterWrite(ESBulkProcessor bulkProcessor)
      throws InterruptedException, IOException {
    BulkProcessorTestUtils.syncAfterWrite(bulkProcessor);
  }

  public static final List<String> SEARCHABLE_ENTITIES;

  static {
    SEARCHABLE_ENTITIES =
        Stream.concat(SEARCHABLE_ENTITY_TYPES.stream(), AUTO_COMPLETE_ENTITY_TYPES.stream())
            .map(EntityTypeMapper::getName)
            .distinct()
            .collect(Collectors.toList());
  }

  public static SearchResult facetAcrossEntities(
      OperationContext opContext,
      SearchService searchService,
      String query,
      @Nullable List<String> facets) {
    return facetAcrossEntities(opContext, searchService, SEARCHABLE_ENTITIES, query, facets, null);
  }

  public static SearchResult facetAcrossEntities(
      OperationContext opContext,
      SearchService searchService,
      List<String> entityNames,
      String query,
      @Nullable List<String> facets,
      @Nullable Filter filter) {
    return searchService.searchAcrossEntities(
        opContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
        entityNames,
        query,
        filter,
        null,
        0,
        100,
        facets);
  }

  public static SearchResult searchAcrossEntities(
      OperationContext opContext, SearchService searchService, String query) {
    return searchAcrossEntities(opContext, searchService, SEARCHABLE_ENTITIES, query, null);
  }

  public static SearchResult searchAcrossEntities(
      OperationContext opContext,
      SearchService searchService,
      List<String> entityNames,
      String query) {
    return searchAcrossEntities(opContext, searchService, entityNames, query, null);
  }

  public static SearchResult searchAcrossEntities(
      OperationContext opContext,
      SearchService searchService,
      List<String> entityNames,
      String query,
      Filter filter) {
    return searchService.searchAcrossEntities(
        opContext.withSearchFlags(
            flags -> flags.setFulltext(true).setSkipCache(true).setSkipHighlighting(false)),
        entityNames,
        query,
        filter,
        null,
        0,
        100,
        null);
  }

  public static SearchResult search(
      OperationContext opContext, SearchService searchService, String query) {
    return search(opContext, searchService, SEARCHABLE_ENTITIES, query);
  }

  public static SearchResult search(
      OperationContext opContext,
      SearchService searchService,
      List<String> entities,
      String query) {
    return searchService.search(
        opContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
        entities,
        query,
        null,
        null,
        0,
        100);
  }

  public static ScrollResult scroll(
      OperationContext opContext,
      SearchService searchService,
      String query,
      int batchSize,
      @Nullable String scrollId) {
    return searchService.scrollAcrossEntities(
        opContext.withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true)),
        SEARCHABLE_ENTITIES,
        query,
        null,
        null,
        scrollId,
        "3m",
        batchSize);
  }

  public static SearchResult searchStructured(
      OperationContext opContext, SearchService searchService, String query) {
    return searchService.searchAcrossEntities(
        opContext.withSearchFlags(flags -> flags.setFulltext(false).setSkipCache(true)),
        SEARCHABLE_ENTITIES,
        query,
        null,
        null,
        0,
        100);
  }

  public static LineageSearchResult lineage(
      OperationContext opContext, LineageSearchService lineageSearchService, Urn root, int hops) {
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
        opContext
            .withSearchFlags(flags -> flags.setSkipCache(true))
            .withLineageFlags(flags -> flags),
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
        100);
  }

  public static AutoCompleteResults autocomplete(
      OperationContext opContext,
      SearchableEntityType<?, String> searchableEntityType,
      String query)
      throws Exception {
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

          @Override
          public OperationContext getOperationContext() {
            return opContext;
          }

          @Override
          public DataHubAppConfiguration getDataHubAppConfig() {
            return new DataHubAppConfiguration();
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

  public static GraphQueryConfiguration getGraphQueryConfiguration() {
    return new GraphQueryConfiguration() {
      {
        setBatchSize(1000);
        setTimeoutSeconds(10);
        setMaxResult(10000);
        setEnableMultiPathSearch(true);
        setBoostViaNodes(true);
      }
    };
  }
}
