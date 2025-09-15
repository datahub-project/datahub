package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
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
import org.testng.annotations.BeforeMethod;
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

  @BeforeMethod
  public void setUp() {
    // Reset mocks before each test to avoid interference
    reset(_graphService, _searchService, _lineageRegistry);

    // Clear cache to avoid interference between tests
    _cacheManager.getCache("test-cache").clear();

    // Re-setup basic mocks that are needed for all tests
    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Re-setup GraphService configuration that was cleared by reset
    GraphServiceConfiguration graphServiceConfig =
        GraphServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(100).build())
                    .build())
            .build();
    when(_graphService.getGraphServiceConfig()).thenReturn(graphServiceConfig);

    // Re-setup GraphService lineage methods that were cleared by reset
    EntityLineageResult mockLineageResult = createMockEntityLineageResult();
    when(_graphService.getImpactLineage(any(), any(), any(), anyInt()))
        .thenReturn(mockLineageResult);
    when(_graphService.getLineage(
            any(), any(), any(LineageDirection.class), anyInt(), anyInt(), anyInt()))
        .thenReturn(mockLineageResult);

    // Re-setup SearchService mock for both method signatures
    SearchResult mockSearchResult = createMockSearchResult();
    when(_searchService.searchAcrossEntities(any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(mockSearchResult);
    when(_searchService.searchAcrossEntities(
            any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(mockSearchResult);
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

  @Test
  public void testLineageVisualizationMode() throws Exception {
    // Test that when LineageFlags has entitiesExploredPerHopLimit > 0,
    // the service calls getLineage instead of getImpactLineage

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Create operation context with lineage flags indicating visualization mode
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(10);
    OperationContext contextWithVisualizationFlags =
        _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response for getLineage call
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(2);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

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

    // Mock the getLineage call (visualization mode)
    when(_graphService.getLineage(
            eq(contextWithVisualizationFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count (from config)
            eq(maxHops)))
        .thenReturn(mockLineageResult);

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithVisualizationFlags,
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

    // Verify that getLineage was called instead of getImpactLineage
    verify(_graphService)
        .getLineage(
            eq(contextWithVisualizationFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count
            eq(maxHops));

    // Verify that getImpactLineage was NOT called
    verify(_graphService, never())
        .getImpactLineage(any(), any(), any(LineageGraphFilters.class), anyInt());
  }

  @Test
  public void testImpactAnalysisMode() throws Exception {
    // Test that when LineageFlags has entitiesExploredPerHopLimit <= 0 or null,
    // the service calls getImpactLineage (default behavior)

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Create operation context with lineage flags indicating impact analysis mode
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(0);
    OperationContext contextWithImpactFlags = _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response for getImpactLineage call
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(2);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithImpactFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithImpactFlags,
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

    // Verify that getImpactLineage was called
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithImpactFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));

    // Verify that getLineage was NOT called
    verify(_graphService, never())
        .getLineage(any(), any(), any(LineageDirection.class), anyInt(), anyInt(), anyInt());
  }

  @Test
  public void testIsLineageVisualizationWithNullFlags() throws Exception {
    // Test that null LineageFlags results in impact analysis mode

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Use operation context with null lineage flags
    OperationContext contextWithNullFlags = _operationContext.withLineageFlags(f -> null);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithNullFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithNullFlags,
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

    // Verify that getImpactLineage was called (impact analysis mode)
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithNullFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));
  }

  @Test
  public void testIsLineageVisualizationWithNullLimit() throws Exception {
    // Test that LineageFlags with null entitiesExploredPerHopLimit results in impact analysis mode

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Create operation context with lineage flags having null limit
    // Note: LineageFlags doesn't allow null values, so we create a flags object without setting the
    // limit
    LineageFlags lineageFlags = new LineageFlags();
    OperationContext contextWithNullLimitFlags =
        _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithNullLimitFlags),
            eq(sourceUrn),
            any(LineageGraphFilters.class),
            eq(maxHops)))
        .thenReturn(mockLineageResult);

    // Call the method under test
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithNullLimitFlags,
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

    // Verify that getImpactLineage was called (impact analysis mode)
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithNullLimitFlags),
            eq(sourceUrn),
            any(LineageGraphFilters.class),
            eq(maxHops));
  }

  @Test
  public void testApplyMaxHopsLimitWithVisualizationMode() throws Exception {
    // Test that applyMaxHopsLimit uses the correct config limit for visualization mode

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);

    // Create operation context with lineage flags indicating visualization mode
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(5);
    OperationContext contextWithVisualizationFlags =
        _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getLineage(
            eq(contextWithVisualizationFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count
            eq(10))) // Should use lineageMaxHops from config (set to 10 in init)
        .thenReturn(mockLineageResult);

    // Call the method under test with null maxHops to trigger applyMaxHopsLimit
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithVisualizationFlags,
            sourceUrn,
            direction,
            entities,
            null, // input
            null, // maxHops - should use config default
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result
    assertNotNull(result);

    // Verify that getLineage was called with the lineageMaxHops limit (10)
    verify(_graphService)
        .getLineage(
            eq(contextWithVisualizationFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count
            eq(10)); // Should use lineageMaxHops from config
  }

  @Test
  public void testApplyMaxHopsLimitWithImpactMode() throws Exception {
    // Test that applyMaxHopsLimit uses the correct config limit for impact analysis mode

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);

    // Create operation context with lineage flags indicating impact analysis mode
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(0);
    OperationContext contextWithImpactFlags = _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithImpactFlags),
            eq(sourceUrn),
            any(LineageGraphFilters.class),
            eq(10))) // Should use impact maxHops from config (set to 10 in init)
        .thenReturn(mockLineageResult);

    // Call the method under test with null maxHops to trigger applyMaxHopsLimit
    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            contextWithImpactFlags,
            sourceUrn,
            direction,
            entities,
            null, // input
            null, // maxHops - should use config default
            null, // inputFilters
            null, // sortCriteria
            0, // from
            10); // size

    // Verify the result
    assertNotNull(result);

    // Verify that getImpactLineage was called with the impact maxHops limit (10)
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithImpactFlags),
            eq(sourceUrn),
            any(LineageGraphFilters.class),
            eq(10)); // Should use impact maxHops from config
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
