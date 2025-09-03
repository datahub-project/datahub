package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.cache.SearchCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchLineageCacheConfiguration;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.ImpactConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LineageSearchServiceTest {

  private LineageSearchService _lineageSearchService;
  private SearchService _searchService;
  private GraphService _graphService;
  private CacheManager _cacheManager;
  private DataHubAppConfiguration _appConfig;
  private OperationContext _operationContext;
  private LineageRegistry _lineageRegistry;

  @BeforeClass
  public void init() {
    _operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Mock dependencies
    _searchService = mock(SearchService.class);
    _graphService = mock(GraphService.class);
    _cacheManager = new ConcurrentMapCacheManager();
    _appConfig = new DataHubAppConfiguration(); // Use real instance instead of mock
    _lineageRegistry = mock(LineageRegistry.class);

    // Create actual configuration objects instead of mocking
    ElasticSearchConfiguration elasticSearchConfig = ElasticSearchConfiguration.builder().build();
    SearchConfiguration searchConfig = SearchConfiguration.builder().build();
    GraphQueryConfiguration graphConfig = GraphQueryConfiguration.builder().build();
    ImpactConfiguration impactConfig = ImpactConfiguration.builder().keepAlive("5m").build();

    // Create cache configuration
    CacheConfiguration cacheConfig =
        CacheConfiguration.builder()
            .search(
                SearchCacheConfiguration.builder()
                    .lineage(
                        SearchLineageCacheConfiguration.builder()
                            .ttlSeconds(600L) // 10 minutes
                            .lightningThreshold(1000L)
                            .build())
                    .build())
            .build();

    // Create GraphService configuration
    GraphServiceConfiguration graphServiceConfig =
        GraphServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(100).build())
                    .build())
            .build();

    // Set up the configuration chain - use real objects and set properties directly
    _appConfig.setElasticSearch(elasticSearchConfig);
    elasticSearchConfig.setSearch(searchConfig);
    searchConfig.setGraph(graphConfig);
    graphConfig.setLineageMaxHops(10); // Default max hops
    graphConfig.setPointInTimeCreationEnabled(true); // Enable PIT for graph queries
    graphConfig.setImpact(impactConfig);
    impactConfig.setMaxHops(10); // Default max hops

    // Set up cache configuration
    _appConfig.setCache(cacheConfig);

    // Create MetadataChangeProposalConfig to avoid NPE
    MetadataChangeProposalConfig metadataChangeProposalConfig =
        MetadataChangeProposalConfig.builder()
            .sideEffects(
                MetadataChangeProposalConfig.SideEffectsConfig.builder()
                    .schemaField(
                        MetadataChangeProposalConfig.SideEffectConfig.builder()
                            .enabled(false)
                            .build())
                    .build())
            .build();
    _appConfig.setMetadataChangeProposal(metadataChangeProposalConfig);

    // Set up GraphService configuration
    when(_graphService.getGraphServiceConfig()).thenReturn(graphServiceConfig);

    // Set up GraphService to return a mock EntityLineageResult
    EntityLineageResult mockLineageResult = createMockEntityLineageResult();
    when(_graphService.getImpactLineage(any(), any(), any(), anyInt()))
        .thenReturn(mockLineageResult);

    // Set up SearchService to return a mock SearchResult
    SearchResult mockSearchResult = createMockSearchResult();
    when(_searchService.searchAcrossEntities(any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(mockSearchResult);
    when(_searchService.searchAcrossEntities(
            any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(mockSearchResult);

    // Create the service under test
    _lineageSearchService =
        new LineageSearchService(
            _searchService, _graphService, _cacheManager.getCache("test-cache"), true, _appConfig);
  }

  @Test
  public void testSearchAcrossLineageWithLineageGraphFilters() throws Exception {
    // Test the new searchAcrossLineage method with LineageGraphFilters

    // Setup test data
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(2);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    // Add some mock relationships
    LineageRelationship rel1 = new LineageRelationship();
    rel1.setEntity(UrnUtils.getUrn("urn:li:dataset:downstream-1"));
    rel1.setType("DownstreamOf");
    rel1.setDegree(1);

    LineageRelationship rel2 = new LineageRelationship();
    rel2.setEntity(UrnUtils.getUrn("urn:li:dataset:downstream-2"));
    rel2.setType("DownstreamOf");
    rel2.setDegree(1);

    mockLineageResult.getRelationships().add(rel1);
    mockLineageResult.getRelationships().add(rel2);

    // Mock the graph service call
    when(_graphService.getImpactLineage(
            eq(_operationContext), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    // Mock the lineage registry
    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            _operationContext,
            sourceUrn,
            direction,
            entities,
            null, // input
            maxHops,
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result
    assertNotNull(result);

    // Verify the graph service was called with correct parameters
    ArgumentCaptor<LineageGraphFilters> filtersCaptor =
        ArgumentCaptor.forClass(LineageGraphFilters.class);
    verify(_graphService)
        .getImpactLineage(
            eq(_operationContext), eq(sourceUrn), filtersCaptor.capture(), eq(maxHops));

    LineageGraphFilters capturedFilters = filtersCaptor.getValue();
    assertEquals(capturedFilters.getLineageDirection(), direction);
  }

  @Test
  public void testSearchAcrossLineageWithUpstreamDirection() throws Exception {
    // Test upstream lineage search

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.UPSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 2;

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    LineageRelationship rel = new LineageRelationship();
    rel.setEntity(UrnUtils.getUrn("urn:li:dataset:upstream-1"));
    rel.setType("Consumes");
    rel.setDegree(1);

    mockLineageResult.getRelationships().add(rel);

    when(_graphService.getImpactLineage(
            eq(_operationContext), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            _operationContext,
            sourceUrn,
            direction,
            entities,
            null, // input
            maxHops,
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result
    assertNotNull(result);

    // Verify upstream direction was used
    ArgumentCaptor<LineageGraphFilters> filtersCaptor =
        ArgumentCaptor.forClass(LineageGraphFilters.class);
    verify(_graphService)
        .getImpactLineage(
            eq(_operationContext), eq(sourceUrn), filtersCaptor.capture(), eq(maxHops));

    LineageGraphFilters capturedFilters = filtersCaptor.getValue();
    assertEquals(capturedFilters.getLineageDirection(), LineageDirection.UPSTREAM);
  }

  @Test
  public void testSearchAcrossLineageWithMaxHopsLimit() throws Exception {
    // Test that maxHops limit is properly enforced

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 1; // Very limited hops

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    LineageRelationship rel = new LineageRelationship();
    rel.setEntity(UrnUtils.getUrn("urn:li:dataset:downstream-1"));
    rel.setType("DownstreamOf");
    rel.setDegree(1); // Only 1 hop

    mockLineageResult.getRelationships().add(rel);

    when(_graphService.getImpactLineage(
            eq(_operationContext), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            _operationContext,
            sourceUrn,
            direction,
            entities,
            null, // input
            maxHops,
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result respects maxHops
    assertNotNull(result);

    // Verify the graph service was called with the correct maxHops
    verify(_graphService)
        .getImpactLineage(
            eq(_operationContext), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));
  }

  @Test
  public void testSearchAcrossLineageWithEmptyResult() throws Exception {
    // Test handling of empty lineage results

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Mock empty response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(_operationContext), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            _operationContext,
            sourceUrn,
            direction,
            entities,
            null, // input
            maxHops,
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify empty result is handled correctly
    assertNotNull(result);
  }

  @Test
  public void testSearchAcrossLineageWithLineageFlags() throws Exception {
    // Test that lineage flags are properly passed through

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 2;

    // Create operation context with lineage flags
    LineageFlags lineageFlags =
        new LineageFlags().setStartTimeMillis(1000L).setEndTimeMillis(2000L);

    OperationContext contextWithFlags = _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test with flags
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithFlags,
            sourceUrn,
            direction,
            entities,
            null, // input
            maxHops,
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result
    assertNotNull(result);

    // Verify the operation context with flags was passed through
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));
  }

  @Test
  public void testLineageGraphFiltersCreation() throws Exception {
    // Test that LineageGraphFilters are created correctly

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);

    // Mock the lineage registry
    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
    _lineageSearchService.searchAcrossLineage(
        _operationContext,
        sourceUrn,
        direction,
        entities,
        null, // input
        1, // maxHops
        null, // inputFilters
        null, // sortCriteria
        0, // from
        10); // size

    // Verify that LineageGraphFilters.forEntityType was used
    ArgumentCaptor<LineageGraphFilters> filtersCaptor =
        ArgumentCaptor.forClass(LineageGraphFilters.class);
    verify(_graphService)
        .getImpactLineage(eq(_operationContext), eq(sourceUrn), filtersCaptor.capture(), eq(1));

    LineageGraphFilters capturedFilters = filtersCaptor.getValue();
    assertEquals(capturedFilters.getLineageDirection(), direction);
  }

  private EntityLineageResult createMockEntityLineageResult() {
    EntityLineageResult result = new EntityLineageResult();
    result.setTotal(0);
    result.setRelationships(new LineageRelationshipArray());
    return result;
  }

  private SearchResult createMockSearchResult() {
    SearchResult result = new SearchResult();
    result.setEntities(new SearchEntityArray());
    result.setMetadata(new SearchResultMetadata());
    result.setFrom(0);
    result.setPageSize(10);
    result.setNumEntities(0);
    return result;
  }
}
