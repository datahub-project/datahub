package io.datahubproject.test.search;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.ImpactConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.awaitility.Awaitility;

public class SearchTestUtils {
  private SearchTestUtils() {}

  public static LimitConfig TEST_1K_LIMIT_CONFIG =
      LimitConfig.builder()
          .results(ResultsLimitConfig.builder().apiDefault(1000).max(1000).build())
          .build();

  public static SearchServiceConfiguration TEST_SEARCH_SERVICE_CONFIG =
      SearchServiceConfiguration.builder().limit(TEST_1K_LIMIT_CONFIG).build();

  public static StructuredPropertiesConfiguration TEST_ES_STRUCT_PROPS_DISABLED =
      StructuredPropertiesConfiguration.builder().enabled(false).systemUpdateEnabled(false).build();

  // Base configuration for tests
  private static final ElasticSearchConfiguration BASE_TEST_CONFIG =
      ElasticSearchConfiguration.builder()
          .search(
              SearchConfiguration.builder()
                  .pointInTimeCreationEnabled(false) // Disable PIT for search entities by default
                  .graph(
                      GraphQueryConfiguration.builder()
                          .batchSize(1000)
                          .timeoutSeconds(5)
                          .enableMultiPathSearch(true)
                          .boostViaNodes(true)
                          .impact(
                              ImpactConfiguration.builder()
                                  .maxHops(1000)
                                  .maxRelations(100)
                                  .slices(2)
                                  .keepAlive("5m")
                                  .searchQueryTimeReservation(0.2) // Default 20% reservation
                                  .build())
                          .lineageMaxHops(20)
                          .maxThreads(1)
                          .queryOptimization(true)
                          .pointInTimeCreationEnabled(true) // Enable PIT for graph queries
                          .build())
                  .build())
          .bulkProcessor(BulkProcessorConfiguration.builder().numRetries(1).build())
          .bulkDelete(
              BulkDeleteConfiguration.builder()
                  .batchSize(1000)
                  .slices("auto")
                  .numRetries(3)
                  .timeout(30)
                  .timeoutUnit("MINUTES")
                  .pollInterval(1)
                  .pollIntervalUnit("SECONDS")
                  .build())
          .index(
              IndexConfiguration.builder()
                  .prefix("")
                  .numShards(1)
                  .numReplicas(1)
                  .numRetries(3)
                  .refreshIntervalSeconds(3)
                  .maxArrayLength(1000)
                  .maxObjectKeys(1000)
                  .maxValueLength(4096)
                  .enableMappingsReindex(true)
                  .enableSettingsReindex(true)
                  .maxReindexHours(0)
                  .minSearchFilterLength(3)
                  .build())
          .buildIndices(
              BuildIndicesConfiguration.builder()
                  .reindexOptimizationEnabled(true)
                  .reindexBatchSize(5000)
                  .reindexMaxSlices(256)
                  .reindexNoProgressRetryMinutes(5)
                  .createIndexRetryEnabled(true)
                  .build())
          .entityIndex(
              EntityIndexConfiguration.builder()
                  .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
                  .v3(
                      EntityIndexVersionConfiguration.builder()
                          .enabled(false)
                          .cleanup(false)
                          .build())
                  .build())
          .build();

  public static ElasticSearchConfiguration TEST_OS_SEARCH_CONFIG = BASE_TEST_CONFIG;

  public static ElasticSearchConfiguration TEST_OS_SEARCH_CONFIG_NO_PIT =
      BASE_TEST_CONFIG.toBuilder()
          .search(
              BASE_TEST_CONFIG.getSearch().toBuilder()
                  .pointInTimeCreationEnabled(false) // Ensure search PIT is disabled
                  .graph(
                      BASE_TEST_CONFIG.getSearch().getGraph().toBuilder()
                          .pointInTimeCreationEnabled(false) // Disable graph PIT for this config
                          .build())
                  .build())
          .build();

  // Configuration with PIT enabled for search entities (for tests that specifically need PIT)
  public static ElasticSearchConfiguration getTestOsSearchConfigWithPit(
      SearchConfiguration searchConfiguration) {
    return BASE_TEST_CONFIG.toBuilder()
        .search(
            searchConfiguration.toBuilder()
                .pointInTimeCreationEnabled(true) // Enable PIT for search entities
                .graph(
                    searchConfiguration.getGraph().toBuilder()
                        .pointInTimeCreationEnabled(true) // Enable graph PIT
                        .build())
                .build())
        .build();
  }

  public static ElasticSearchConfiguration TEST_ES_SEARCH_CONFIG =
      TEST_OS_SEARCH_CONFIG.toBuilder().build();

  public static SystemMetadataServiceConfig TEST_SYSTEM_METADATA_SERVICE_CONFIG =
      SystemMetadataServiceConfig.builder().limit(TEST_1K_LIMIT_CONFIG).build();

  public static GraphServiceConfiguration TEST_GRAPH_SERVICE_CONFIG =
      GraphServiceConfiguration.builder().limit(TEST_1K_LIMIT_CONFIG).build();

  public static TimeseriesAspectServiceConfig TEST_TIMESERIES_ASPECT_SERVICE_CONFIG =
      TimeseriesAspectServiceConfig.builder().limit(TEST_1K_LIMIT_CONFIG).build();

  public static void syncAfterWrite(ESBulkProcessor bulkProcessor)
      throws InterruptedException, IOException {
    BulkProcessorTestUtils.syncAfterWrite(bulkProcessor);
  }

  public static final List<String> SEARCHABLE_ENTITIES;

  static {
    SEARCHABLE_ENTITIES =
        Stream.concat(
                SearchUtils.SEARCHABLE_ENTITY_TYPES.stream(),
                SearchUtils.AUTO_COMPLETE_ENTITY_TYPES.stream())
            .map(EntityTypeMapper::getName)
            .distinct()
            .collect(Collectors.toList());
  }

  /**
   * Default EntityIndexConfiguration for testing with V2 enabled and V3 disabled. This is the most
   * common configuration used in tests.
   */
  public static final EntityIndexConfiguration DEFAULT_ENTITY_INDEX_CONFIGURATION =
      EntityIndexConfiguration.builder()
          .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
          .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
          .build();

  /**
   * EntityIndexConfiguration for testing with both V2 and V3 enabled. This configuration matches
   * the default values from application.yaml: - V2: enabled=true, cleanup=false - V3: enabled=true,
   * cleanup=false, analyzerConfig=search_entity_analyzer_config.yaml,
   * mappingConfig=search_entity_mapping_config.yaml, maxFieldsLimit=5000
   */
  public static final EntityIndexConfiguration V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION =
      EntityIndexConfiguration.builder()
          .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
          .v3(
              EntityIndexVersionConfiguration.builder()
                  .enabled(true)
                  .cleanup(true)
                  .analyzerConfig("search_entity_analyzer_config.yaml")
                  .mappingConfig("search_entity_mapping_config.yaml")
                  .maxFieldsLimit(5000)
                  .build())
          .build();

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
        List.of());
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

  public static ScrollResult scrollAcrossEntities(
      OperationContext opContext, SearchService searchService, String query) {
    return scrollAcrossEntities(opContext, searchService, SEARCHABLE_ENTITIES, query, null);
  }

  public static ScrollResult scrollAcrossEntities(
      OperationContext opContext,
      SearchService searchService,
      List<String> entityNames,
      String query,
      Filter filter) {
    return searchService.scrollAcrossEntities(
        opContext.withSearchFlags(
            flags -> flags.setFulltext(true).setSkipCache(true).setSkipHighlighting(false)),
        entityNames,
        query,
        filter,
        null,
        null,
        null,
        100);
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
        SearchUtils.SEARCHABLE_ENTITY_TYPES.stream()
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

          @Override
          public int getMaxParentDepth() {
            return 50;
          }
        });
  }

  public static SearchClientShim<?> environmentRestClientBuilder() throws IOException {
    Integer port =
        Integer.parseInt(Optional.ofNullable(System.getenv("ELASTICSEARCH_PORT")).orElse("9200"));
    SearchClientShim.ShimConfiguration shimConfiguration =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost(Optional.ofNullable(System.getenv("ELASTICSEARCH_HOST")).orElse("localhost"))
            .withPort(port)
            .withSSL(port.equals(443))
            .withCredentials(
                System.getenv("ELASTICSEARCH_USERNAME"), System.getenv("ELASTICSEARCH_PASSWORD"))
            .build();

    return SearchClientShimUtil.createShim(shimConfiguration, new ObjectMapper());
  }

  /**
   * Generic method to wait for data availability using a custom verification function. This method
   * uses Awaitility to retry the verification function until it returns true or the timeout is
   * reached.
   *
   * @param verificationFunction The function to call for verification (should return true when data
   *     is ready)
   * @param maxWaitTimeSeconds Maximum time to wait in seconds
   * @param errorMessage The error message to throw if data isn't available within the timeout
   */
  public static void waitForDataAvailability(
      DataAvailabilityChecker verificationFunction, int maxWaitTimeSeconds, String errorMessage) {

    try {
      Awaitility.await()
          .timeout(Duration.ofSeconds(maxWaitTimeSeconds))
          .pollInterval(Duration.ofSeconds(1))
          .until(
              () -> {
                try {
                  return verificationFunction.check();
                } catch (Exception e) {
                  return false;
                }
              });
    } catch (org.awaitility.core.ConditionTimeoutException e) {
      throw new RuntimeException(errorMessage, e);
    }
  }

  /** Functional interface for data availability checking. */
  @FunctionalInterface
  public interface DataAvailabilityChecker {
    boolean check() throws Exception;
  }

  /**
   * Creates a DelegatingSettingsBuilder with the appropriate settings builders based on the entity
   * index configuration. This utility method encapsulates the common pattern of creating a list of
   * SettingsBuilder implementations and passing them to DelegatingSettingsBuilder constructor.
   *
   * @param entityIndexConfiguration the entity index configuration to determine which builders to
   *     include
   * @param indexConfiguration the index configuration for LegacySettingsBuilder
   * @param indexConvention the index convention for the builders
   * @return a DelegatingSettingsBuilder instance
   * @throws RuntimeException if MultiEntitySettingsBuilder initialization fails
   */
  public static DelegatingSettingsBuilder createDelegatingSettingsBuilder(
      EntityIndexConfiguration entityIndexConfiguration,
      IndexConfiguration indexConfiguration,
      IndexConvention indexConvention) {

    List<SettingsBuilder> settingsBuilders = new ArrayList<>();

    if (entityIndexConfiguration.getV2().isEnabled()) {
      settingsBuilders.add(new V2LegacySettingsBuilder(indexConfiguration, indexConvention));
    }
    if (entityIndexConfiguration.getV3().isEnabled()) {
      try {
        settingsBuilders.add(
            new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention));
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize MultiEntitySettingsBuilder", e);
      }
    }

    return new DelegatingSettingsBuilder(settingsBuilders);
  }

  /**
   * Creates a DelegatingMappingsBuilder with the appropriate mappings builders based on the entity
   * index configuration. This utility method encapsulates the common pattern of creating a list of
   * MappingsBuilder implementations and passing them to DelegatingMappingsBuilder constructor.
   *
   * @param entityIndexConfiguration the entity index configuration to determine which builders to
   *     include
   * @return a DelegatingMappingsBuilder instance
   * @throws RuntimeException if MultiEntityMappingsBuilder initialization fails
   */
  public static DelegatingMappingsBuilder createDelegatingMappingsBuilder(
      EntityIndexConfiguration entityIndexConfiguration) {

    List<MappingsBuilder> builders = new ArrayList<>();

    if (entityIndexConfiguration.getV2().isEnabled()) {
      builders.add(new V2MappingsBuilder(entityIndexConfiguration));
    }
    if (entityIndexConfiguration.getV3().isEnabled()) {
      try {
        builders.add(new MultiEntityMappingsBuilder(entityIndexConfiguration));
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
      }
    }

    // Add NoOpMappingsBuilder as fallback
    builders.add(new NoOpMappingsBuilder());

    return new DelegatingMappingsBuilder(builders);
  }
}
