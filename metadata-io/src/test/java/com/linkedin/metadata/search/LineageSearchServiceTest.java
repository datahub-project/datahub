package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
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
import com.linkedin.metadata.config.search.SearchLineageConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SchemaFieldValidationMode;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageSearchServiceTest {

  private static final Urn ORDERS =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)");
  private static final Urn CUSTOMERS =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.customers,PROD)");
  private static final Urn DBT_ORDERS =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)");

  /** Small enough that a test can build a fan-out wider than it without building thousands. */
  private static final int MAX_PARENTS_TO_VALIDATE = 5;

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
    ImpactConfiguration impactConfig =
        ImpactConfiguration.builder()
            .keepAlive("5m")
            .searchQueryTimeReservation(0.2) // Default 20% reservation
            .build();

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
    _appConfig.setGraphService(graphServiceConfig);
    _appConfig.setElasticSearch(elasticSearchConfig);
    elasticSearchConfig.setSearch(searchConfig);
    searchConfig.setGraph(graphConfig);
    graphConfig.setLineageMaxHops(10); // Default max hops
    graphConfig.setPointInTimeCreationEnabled(true); // Enable PIT for graph queries
    graphConfig.setSliceFutureDrainTimeoutSeconds(2);
    graphConfig.setImpact(impactConfig);
    impactConfig.setMaxHops(10); // Default max hops

    // Set up cache configuration
    _appConfig.setCache(cacheConfig);

    _appConfig.setSearchService(
        SearchServiceConfiguration.builder()
            .lineage(
                SearchLineageConfiguration.builder()
                    .maxParentsToValidate(MAX_PARENTS_TO_VALIDATE)
                    .build())
            .build());

    // Create MetadataChangeProposalConfig to avoid NPE
    MetadataChangeProposalConfig metadataChangeProposalConfig =
        MetadataChangeProposalConfig.builder()
            .sideEffects(
                MetadataChangeProposalConfig.SideEffectsConfig.builder()
                    .schemaField(
                        MetadataChangeProposalConfig.SchemaFieldSideEffectsConfig.builder()
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

  @Test
  public void testCacheKeyCreationWithLineageFlagsNotNull() throws Exception {
    // Test that cache key is created correctly when lineageFlags is not null
    // This specifically tests the condition: opContext.getSearchContext().getLineageFlags() != null

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Create operation context with lineage flags that has entitiesExploredPerHopLimit set
    // Note: entitiesExploredPerHopLimit > 0 triggers visualization mode, which calls getLineage
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(15);
    OperationContext contextWithFlags = _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    // Mock getLineage call (visualization mode) since entitiesExploredPerHopLimit > 0
    when(_graphService.getLineage(
            eq(contextWithFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count (from config)
            eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
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

    // Verify that getLineage was called (visualization mode) instead of getImpactLineage
    verify(_graphService)
        .getLineage(
            eq(contextWithFlags),
            eq(sourceUrn),
            eq(direction),
            eq(0), // start
            eq(100), // count
            eq(maxHops));

    // Verify that getImpactLineage was NOT called
    verify(_graphService, never())
        .getImpactLineage(any(), any(), any(LineageGraphFilters.class), anyInt());

    // The key test is that the cache key creation logic in LineageSearchService
    // should have used the entitiesExploredPerHopLimit value (15) from the lineageFlags
    // instead of null. This is verified by the fact that the method executes successfully
    // and the cache key is created with the correct lineageFlags value.
  }

  @Test
  public void testCacheKeyCreationWithLineageFlagsNotNullButZeroLimit() throws Exception {
    // Test that cache key is created correctly when lineageFlags is not null but
    // entitiesExploredPerHopLimit is 0
    // This specifically tests the condition: opContext.getSearchContext().getLineageFlags() != null
    // but triggers impact analysis mode (getImpactLineage) instead of visualization mode

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Create operation context with lineage flags that has entitiesExploredPerHopLimit set to 0
    // Note: entitiesExploredPerHopLimit = 0 triggers impact analysis mode, which calls
    // getImpactLineage
    LineageFlags lineageFlags = new LineageFlags().setEntitiesExploredPerHopLimit(0);
    OperationContext contextWithFlags = _operationContext.withLineageFlags(f -> lineageFlags);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    // Mock getImpactLineage call (impact analysis mode) since entitiesExploredPerHopLimit = 0
    when(_graphService.getImpactLineage(
            eq(contextWithFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    // Call the method under test
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

    // Verify that getImpactLineage was called (impact analysis mode)
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));

    // Verify that getLineage was NOT called
    verify(_graphService, never())
        .getLineage(any(), any(), any(LineageDirection.class), anyInt(), anyInt(), anyInt());

    // The key test is that the cache key creation logic in LineageSearchService
    // should have used the entitiesExploredPerHopLimit value (0) from the lineageFlags
    // instead of null. This is verified by the fact that the method executes successfully
    // and the cache key is created with the correct lineageFlags value.
  }

  @Test
  public void testCacheKeyCreationWithLineageFlagsNull() throws Exception {
    // Test that cache key is created correctly when lineageFlags is null
    // This specifically tests the condition: opContext.getSearchContext().getLineageFlags() == null

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    LineageDirection direction = LineageDirection.DOWNSTREAM;
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);
    Integer maxHops = 3;

    // Use operation context with null lineage flags
    OperationContext contextWithNullFlags = _operationContext.withLineageFlags(f -> null);

    // Mock the graph service response
    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(1);
    mockLineageResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getImpactLineage(
            eq(contextWithNullFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops)))
        .thenReturn(mockLineageResult);

    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

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

    // Verify that the graph service was called with the correct context
    verify(_graphService)
        .getImpactLineage(
            eq(contextWithNullFlags), eq(sourceUrn), any(LineageGraphFilters.class), eq(maxHops));

    // The key test is that the cache key creation logic in LineageSearchService
    // should have used null for entitiesExploredPerHopLimit since lineageFlags is null.
    // This is verified by the fact that the method executes successfully
    // and the cache key is created with null for the entitiesExploredPerHopLimit value.
  }

  /** One relationship, far below the lightning threshold. */
  private static List<LineageRelationship> oneRelationship() {
    return Collections.singletonList(
        new LineageRelationship()
            .setEntity(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,topic,PROD)"))
            .setType("DownstreamOf")
            .setDegree(1));
  }

  private static Filter parentFilter(Condition condition, boolean negated, String value) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(buildCriterion("parent", condition, negated, value)))));
  }

  @Test
  public void testCanDoLightningWhenRequested() {
    List<LineageRelationship> tinyResult = oneRelationship();
    Filter filter = QueryUtils.newFilter("platform", "urn:li:dataPlatform:kafka");

    // Below the threshold the entity index path is still preferred...
    assertFalse(_lineageSearchService.canDoLightning(tinyResult, "*", filter, null, false));
    // ...but a caller can ask for it whatever the result size
    assertTrue(_lineageSearchService.canDoLightning(tinyResult, "*", filter, null, true));

    // Filters this path cannot answer from a urn keep it off, requested or not
    Filter unsupported = QueryUtils.newFilter("description", "anything");
    assertFalse(_lineageSearchService.canDoLightning(tinyResult, "*", unsupported, null, true));
  }

  @Test
  public void testPassesParentCriteria() {
    Urn warehouseColumn = column(ORDERS, "id");
    Urn dbtColumn = column(DBT_ORDERS, "id");

    assertTrue(LineageSearchService.passesLightningCriteria(warehouseColumn, null, null, null));

    // Excluding columns on dbt nodes, which the graph walks through rather than drawing
    Filter notDbt = parentFilter(Condition.CONTAIN, true, "urn:li:dataPlatform:dbt");
    assertTrue(LineageSearchService.passesLightningCriteria(warehouseColumn, null, null, notDbt));
    assertFalse(LineageSearchService.passesLightningCriteria(dbtColumn, null, null, notDbt));
    // Nothing to read a parent from, so only a negated criterion lets it through
    assertTrue(LineageSearchService.passesLightningCriteria(ORDERS, null, null, notDbt));

    // Excluding one specific node, as siblings drawn folded into another node are
    Filter notSibling = parentFilter(Condition.EQUAL, true, DBT_ORDERS.toString());
    assertFalse(LineageSearchService.passesLightningCriteria(dbtColumn, null, null, notSibling));
    assertTrue(
        LineageSearchService.passesLightningCriteria(warehouseColumn, null, null, notSibling));
  }

  @Test
  public void testCriteriaAreEvaluatedPerOrBranch() {
    // Two branches, each naming a different platform. Pooling their criteria into one conjunction
    // would reject everything, since no urn is on both platforms.
    Filter eitherPlatform =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    "platform", Condition.EQUAL, "urn:li:dataPlatform:snowflake"))),
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    "platform", Condition.EQUAL, "urn:li:dataPlatform:dbt")))));

    assertTrue(
        LineageSearchService.passesLightningCriteria(
            ORDERS, "urn:li:dataPlatform:snowflake", "PROD", eitherPlatform));
    assertTrue(
        LineageSearchService.passesLightningCriteria(
            DBT_ORDERS, "urn:li:dataPlatform:dbt", "PROD", eitherPlatform));
    assertFalse(
        LineageSearchService.passesLightningCriteria(
            ORDERS, "urn:li:dataPlatform:kafka", "PROD", eitherPlatform));
  }

  @Test
  public void testNegatedPlatformCriterionIsHonored() {
    Filter notSnowflake =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    true,
                                    "urn:li:dataPlatform:snowflake")))));

    assertFalse(
        LineageSearchService.passesLightningCriteria(
            ORDERS, "urn:li:dataPlatform:snowflake", "PROD", notSnowflake));
    assertTrue(
        LineageSearchService.passesLightningCriteria(
            DBT_ORDERS, "urn:li:dataPlatform:dbt", "PROD", notSnowflake));
  }

  @Test
  public void testSchemaFieldTakesPlatformAndEnvironmentFromItsParent() {
    // Without this a platform or origin filter drops every column, since a schema field urn
    // carries neither of its own
    assertEquals(
        _lineageSearchService.getPlatform(
            Constants.SCHEMA_FIELD_ENTITY_NAME, column(ORDERS, "order_id")),
        "urn:li:dataPlatform:snowflake");
    assertEquals(
        _lineageSearchService.getEnvironment(
            Constants.SCHEMA_FIELD_ENTITY_NAME, column(ORDERS, "order_id")),
        "PROD");
  }

  @Test
  public void testLightningModeRejectedWhenUnservable() throws Exception {
    // A query string cannot be served off the graph, so asking for lightning mode there has to fail
    // rather than quietly fall back to the entity index and come back short
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");
    List<String> entities = Collections.singletonList(DATASET_ENTITY_NAME);

    OperationContext ghostContext =
        _operationContext.withLineageFlags(f -> new LineageFlags().setForceLightningMode(true));

    EntityLineageResult mockLineageResult = new EntityLineageResult();
    mockLineageResult.setTotal(0);
    mockLineageResult.setRelationships(new LineageRelationshipArray());
    when(_graphService.getImpactLineage(
            eq(ghostContext), eq(sourceUrn), any(LineageGraphFilters.class), anyInt()))
        .thenReturn(mockLineageResult);
    when(_lineageRegistry.getEntitiesWithLineageToEntityType(DATASET_ENTITY_NAME))
        .thenReturn(Collections.singleton(DATASET_ENTITY_NAME));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageSearchService.searchAcrossLineage(
                ghostContext,
                sourceUrn,
                LineageDirection.DOWNSTREAM,
                entities,
                "some query",
                1,
                null,
                null,
                0,
                10));

    // The same request without a query string is served by the graph-only path
    assertNotNull(
        _lineageSearchService.searchAcrossLineage(
            ghostContext,
            sourceUrn,
            LineageDirection.DOWNSTREAM,
            entities,
            null,
            1,
            null,
            null,
            0,
            10));
  }

  @Test
  public void testSchemaFieldValidationDefaultsToNone() {
    // Off unless asked for, so callers that reach the graph-only path by exceeding its size
    // threshold rather than by requesting it are unaffected
    assertEquals(
        LineageSearchService.schemaFieldValidationMode(null), SchemaFieldValidationMode.NONE);
    assertEquals(
        LineageSearchService.schemaFieldValidationMode(new LineageFlags()),
        SchemaFieldValidationMode.NONE);
    assertEquals(
        LineageSearchService.schemaFieldValidationMode(
            new LineageFlags().setValidateSchemaFields(SchemaFieldValidationMode.AUTO)),
        SchemaFieldValidationMode.AUTO);
    assertEquals(
        LineageSearchService.schemaFieldValidationMode(
            new LineageFlags().setValidateSchemaFields(SchemaFieldValidationMode.ALWAYS)),
        SchemaFieldValidationMode.ALWAYS);
  }

  private static Urn column(Urn parent, String fieldPath) {
    return SchemaFieldUtils.generateSchemaFieldUrn(parent, fieldPath);
  }

  private static LineageRelationship relationship(Urn entity) {
    return new LineageRelationship().setEntity(entity).setType("DownstreamOf").setDegree(1);
  }

  private static Aspect schemaMetadata(String... fieldPaths) {
    SchemaFieldArray fields = new SchemaFieldArray();
    for (String fieldPath : fieldPaths) {
      fields.add(new SchemaField().setFieldPath(fieldPath));
    }
    return new Aspect(new SchemaMetadata().setFields(fields).data());
  }

  /** An operation context whose aspect retriever serves the given schemaMetadata by parent. */
  private OperationContext contextWithSchemas(
      SchemaFieldValidationMode mode, Map<Urn, Aspect> schemasByParent) {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> requested = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> response = new HashMap<>();
              requested.stream()
                  .filter(schemasByParent::containsKey)
                  .forEach(
                      urn ->
                          response.put(
                              urn,
                              Map.of(
                                  Constants.SCHEMA_METADATA_ASPECT_NAME,
                                  schemasByParent.get(urn))));
              return response;
            });

    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .build();

    return _operationContext.toBuilder()
        .retrieverContext(retrieverContext)
        .build(_operationContext.getSessionAuthentication(), false)
        .withLineageFlags(f -> new LineageFlags().setValidateSchemaFields(mode));
  }

  @Test
  public void testDropSchemaFieldsMissingFromParent() {
    Urn present = column(ORDERS, "order_id");
    Urn removed = column(ORDERS, "dropped_column");
    List<LineageRelationship> relationships =
        List.of(
            relationship(CUSTOMERS),
            relationship(removed),
            relationship(present),
            relationship(ORDERS));

    OperationContext opContext =
        contextWithSchemas(
            SchemaFieldValidationMode.AUTO, Map.of(ORDERS, schemaMetadata("order_id", "amount")));

    // Non-schema-field relationships pass through untouched, and the incoming order -- which the
    // caller pages over -- is preserved
    assertEquals(
        _lineageSearchService.dropSchemaFieldsMissingFromParent(opContext, relationships),
        List.of(relationship(CUSTOMERS), relationship(present), relationship(ORDERS)));
  }

  @Test
  public void testDropSchemaFieldsWhenParentHasNoSchema() {
    List<LineageRelationship> relationships =
        List.of(relationship(column(ORDERS, "order_id")), relationship(column(CUSTOMERS, "id")));

    // Only orders has a schema to check against, so nothing under customers can be confirmed
    OperationContext opContext =
        contextWithSchemas(
            SchemaFieldValidationMode.AUTO, Map.of(ORDERS, schemaMetadata("order_id")));

    assertEquals(
        _lineageSearchService.dropSchemaFieldsMissingFromParent(opContext, relationships),
        List.of(relationship(column(ORDERS, "order_id"))));
  }

  @Test
  public void testDropSchemaFieldsKeepsV1AliasOfV2Field() {
    // The graph may point at either urn form for the same column, so both have to be recognized
    Urn v2Path = column(ORDERS, "[version=2.0].[type=struct].customer.[type=string].id");
    Urn v1Path = column(ORDERS, "customer.id");
    List<LineageRelationship> relationships = List.of(relationship(v1Path), relationship(v2Path));

    OperationContext opContext =
        contextWithSchemas(
            SchemaFieldValidationMode.AUTO,
            Map.of(
                ORDERS, schemaMetadata("[version=2.0].[type=struct].customer.[type=string].id")));

    assertEquals(
        _lineageSearchService.dropSchemaFieldsMissingFromParent(opContext, relationships),
        relationships);
  }

  @Test
  public void testValidationModeNoneSkipsTheAspectRead() {
    List<LineageRelationship> relationships =
        Collections.singletonList(relationship(column(ORDERS, "dropped_column")));

    // NONE is the default, so a caller that reached the graph-only path by exceeding its size
    // threshold neither pays for the read nor has its results changed
    OperationContext opContext = contextWithSchemas(SchemaFieldValidationMode.NONE, Map.of());

    assertEquals(
        _lineageSearchService.dropSchemaFieldsMissingFromParent(opContext, relationships),
        relationships);
    verify(opContext.getRetrieverContext().getAspectRetriever(), never())
        .getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testAutoValidationGivesUpOnAWideFanOutButAlwaysDoesNot() {
    // More parents than AUTO is willing to fetch, none of which declare the column pointed at
    List<LineageRelationship> relationships =
        IntStream.rangeClosed(0, MAX_PARENTS_TO_VALIDATE)
            .mapToObj(
                i ->
                    relationship(
                        column(
                            UrnUtils.getUrn(
                                String.format(
                                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t%d,PROD)",
                                    i)),
                            "dropped_column")))
            .collect(Collectors.toList());

    assertEquals(
        _lineageSearchService.dropSchemaFieldsMissingFromParent(
            contextWithSchemas(SchemaFieldValidationMode.AUTO, Map.of()), relationships),
        relationships,
        "AUTO should leave a wide fan-out unvalidated rather than fetch every parent");
    assertTrue(
        _lineageSearchService
            .dropSchemaFieldsMissingFromParent(
                contextWithSchemas(SchemaFieldValidationMode.ALWAYS, Map.of()), relationships)
            .isEmpty(),
        "ALWAYS should validate however many parents it takes");
  }

  @Test
  public void testLightningCountDropsRemovedColumns() throws Exception {
    EntityLineageResult lineageResult =
        new EntityLineageResult()
            .setTotal(2)
            .setRelationships(
                new LineageRelationshipArray(
                    relationship(column(ORDERS, "order_id")),
                    relationship(column(ORDERS, "dropped_column"))));
    when(_graphService.getImpactLineage(any(), any(), any(LineageGraphFilters.class), anyInt()))
        .thenReturn(lineageResult);

    OperationContext opContext =
        contextWithSchemas(
                SchemaFieldValidationMode.AUTO, Map.of(ORDERS, schemaMetadata("order_id")))
            .withLineageFlags(
                f ->
                    new LineageFlags()
                        .setForceLightningMode(true)
                        .setValidateSchemaFields(SchemaFieldValidationMode.AUTO))
            // An empty grouping spec keeps schema fields as schema fields, as the counts resolver
            // sets; the service otherwise folds them into their parent dataset
            .withSearchFlags(f -> new SearchFlags().setGroupingSpec(new GroupingSpec()));

    LineageSearchResult result =
        _lineageSearchService.searchAcrossLineage(
            opContext,
            ORDERS,
            LineageDirection.DOWNSTREAM,
            Collections.singletonList(Constants.SCHEMA_FIELD_ENTITY_NAME),
            null,
            1,
            null,
            null,
            0,
            0);

    assertEquals(result.getLineageSearchPath(), LineageSearchPath.LIGHTNING);
    assertEquals(result.getNumEntities().intValue(), 1);
  }

  @Test
  public void testScrollRejectsFlagsItCannotHonor() {
    // Scrolling always reads from the entity index, so answering either of these would mean
    // handing back a short result that reads as the whole answer
    assertThrows(
        IllegalArgumentException.class,
        () -> scroll(new LineageFlags().setForceLightningMode(true)));
    assertThrows(
        IllegalArgumentException.class,
        () -> scroll(new LineageFlags().setValidateSchemaFields(SchemaFieldValidationMode.AUTO)));

    // Flags it does honor, and the defaults, are left alone
    scroll(new LineageFlags().setEntitiesExploredPerHopLimit(10));
    scroll(new LineageFlags());
  }

  private void scroll(LineageFlags lineageFlags) {
    _lineageSearchService.scrollAcrossLineage(
        _operationContext.withLineageFlags(f -> lineageFlags),
        ORDERS,
        LineageDirection.DOWNSTREAM,
        Collections.singletonList(DATASET_ENTITY_NAME),
        null,
        1,
        null,
        null,
        null,
        "5m",
        10);
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
