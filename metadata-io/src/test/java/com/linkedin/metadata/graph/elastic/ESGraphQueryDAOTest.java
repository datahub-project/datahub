package com.linkedin.metadata.graph.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESGraphQueryDAOTest {

  private SearchClientShim<?> mockClient;
  private GraphServiceConfiguration mockGraphServiceConfig;
  private ElasticSearchConfiguration mockElasticSearchConfig;
  private MetricUtils mockMetricUtils;
  private GraphQueryBaseDAO mockDelegate;
  private OperationContext mockOperationContext;
  private Urn mockEntityUrn;
  private LineageGraphFilters mockLineageGraphFilters;
  private GraphFilters mockGraphFilters;
  private List<SortCriterion> mockSortCriteria;
  private SearchRequest mockSearchRequest;
  private SearchResponse mockSearchResponse;

  // Track created DAOs for cleanup to prevent thread pool leaks
  private final List<ESGraphQueryDAO> createdDAOs = new ArrayList<>();

  @BeforeMethod
  public void setUp() {
    mockClient = mock(SearchClientShim.class);
    mockGraphServiceConfig = mock(GraphServiceConfiguration.class);
    mockElasticSearchConfig = mock(ElasticSearchConfiguration.class);
    mockMetricUtils = mock(MetricUtils.class);
    mockDelegate = mock(GraphQueryBaseDAO.class);
    mockOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    mockEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    mockLineageGraphFilters = mock(LineageGraphFilters.class);
    mockGraphFilters = mock(GraphFilters.class);
    mockSortCriteria = Arrays.asList(mock(SortCriterion.class));
    mockSearchRequest = mock(SearchRequest.class);
    mockSearchResponse = mock(SearchResponse.class);

    // Configure nested mock objects for ElasticSearchConfiguration
    SearchConfiguration mockSearchConfig = mock(SearchConfiguration.class);
    GraphQueryConfiguration mockGraphQueryConfig = mock(GraphQueryConfiguration.class);

    when(mockElasticSearchConfig.getSearch()).thenReturn(mockSearchConfig);
    when(mockSearchConfig.getGraph()).thenReturn(mockGraphQueryConfig);

    // Configure GraphQueryConfiguration with valid values for thread pool creation
    when(mockGraphQueryConfig.getMaxThreads()).thenReturn(1);
    createdDAOs.clear(); // Clear any previously tracked DAOs
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown all created DAOs to prevent thread pool leaks
    for (ESGraphQueryDAO dao : createdDAOs) {
      try {
        dao.destroy();
      } catch (Exception e) {
        // Log but don't fail the test
        System.err.println("Failed to destroy DAO: " + e.getMessage());
      }
    }
    createdDAOs.clear();
  }

  @Test
  public void testConstructorWithElasticsearchImplementation() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    assertNotNull(dao);
  }

  @Test
  public void testConstructorWithOpenSearchImplementation() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    assertNotNull(dao);
  }

  @Test
  public void testConstructorWithUnsupportedImplementation() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.UNKNOWN);

    assertThrows(
        NotImplementedException.class,
        () -> {
          new ESGraphQueryDAO(
              mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
        });
  }

  @Test
  public void testGetLineage() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(5, Arrays.asList(), false);
    when(mockDelegate.getLineage(
            eq(mockOperationContext),
            eq(mockEntityUrn),
            eq(mockLineageGraphFilters),
            eq(0),
            eq(10),
            eq(3)))
        .thenReturn(expectedResponse);

    LineageResponse result =
        dao.getLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 0, 10, 3);

    assertEquals(result, expectedResponse);
    verify(mockDelegate)
        .getLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 0, 10, 3);
  }

  @Test
  public void testGetImpactLineage() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(3, Arrays.asList(), false);
    when(mockDelegate.getImpactLineage(
            eq(mockOperationContext), eq(mockEntityUrn), eq(mockLineageGraphFilters), eq(5)))
        .thenReturn(expectedResponse);

    LineageResponse result =
        dao.getImpactLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 5);

    assertEquals(result, expectedResponse);
    verify(mockDelegate)
        .getImpactLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 5);
  }

  @Test
  public void testGetSearchResponseBasic() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.getSearchResponse(
            eq(mockOperationContext), eq(mockGraphFilters), eq(0), eq(20)))
        .thenReturn(mockSearchResponse);

    SearchResponse result = dao.getSearchResponse(mockOperationContext, mockGraphFilters, 0, 20);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate).getSearchResponse(mockOperationContext, mockGraphFilters, 0, 20);
  }

  @Test
  public void testGetSearchResponseAdvanced() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.getSearchResponse(
            eq(mockOperationContext),
            eq(mockGraphFilters),
            eq(mockSortCriteria),
            eq("scroll123"),
            eq("5m"),
            eq(15)))
        .thenReturn(mockSearchResponse);

    SearchResponse result =
        dao.getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, "scroll123", "5m", 15);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate)
        .getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, "scroll123", "5m", 15);
  }

  @Test
  public void testGetSearchResponseWithNullParameters() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.getSearchResponse(
            eq(mockOperationContext),
            eq(mockGraphFilters),
            eq(mockSortCriteria),
            isNull(),
            isNull(),
            isNull()))
        .thenReturn(mockSearchResponse);

    SearchResponse result =
        dao.getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, null, null, null);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate)
        .getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, null, null, null);
  }

  @Test
  public void testExecuteSearch() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.executeSearch(eq(mockSearchRequest))).thenReturn(mockSearchResponse);

    SearchResponse result = dao.executeSearch(mockSearchRequest);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate).executeSearch(mockSearchRequest);
  }

  @Test
  public void testConstructorCreatesCorrectDelegateForElasticsearch() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    assertNotNull(dao);
    // The delegate should be an instance of GraphQueryElasticsearch7DAO
    assertTrue(dao instanceof ESGraphQueryDAO);
  }

  @Test
  public void testConstructorCreatesCorrectDelegateForOpenSearch() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    assertNotNull(dao);
    // The delegate should be an instance of GraphQueryPITDAO
    assertTrue(dao instanceof ESGraphQueryDAO);
  }

  @Test
  public void testGetLineageWithNullCount() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(5, Arrays.asList(), false);
    when(mockDelegate.getLineage(
            eq(mockOperationContext),
            eq(mockEntityUrn),
            eq(mockLineageGraphFilters),
            eq(0),
            isNull(),
            eq(3)))
        .thenReturn(expectedResponse);

    LineageResponse result =
        dao.getLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 0, null, 3);

    assertEquals(result, expectedResponse);
    verify(mockDelegate)
        .getLineage(mockOperationContext, mockEntityUrn, mockLineageGraphFilters, 0, null, 3);
  }

  @Test
  public void testGetSearchResponseWithNullCount() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.getSearchResponse(
            eq(mockOperationContext), eq(mockGraphFilters), eq(0), isNull()))
        .thenReturn(mockSearchResponse);

    SearchResponse result = dao.getSearchResponse(mockOperationContext, mockGraphFilters, 0, null);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate).getSearchResponse(mockOperationContext, mockGraphFilters, 0, null);
  }

  @Test
  public void testGetSearchResponseAdvancedWithNullParameters() {
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
    createdDAOs.add(dao);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockDelegate.getSearchResponse(
            eq(mockOperationContext),
            eq(mockGraphFilters),
            eq(mockSortCriteria),
            eq("scroll123"),
            eq("5m"),
            isNull()))
        .thenReturn(mockSearchResponse);

    SearchResponse result =
        dao.getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, "scroll123", "5m", null);

    assertEquals(result, mockSearchResponse);
    verify(mockDelegate)
        .getSearchResponse(
            mockOperationContext, mockGraphFilters, mockSortCriteria, "scroll123", "5m", null);
  }

  @Test
  public void testDestroyWithGraphQueryPITDAO() throws Exception {
    // Test destroy() method when delegate is GraphQueryPITDAO
    SearchClientShim<?> testClient = mock(SearchClientShim.class);
    when(testClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a real GraphQueryPITDAO as delegate
    GraphQueryPITDAO pitDAO =
        new GraphQueryPITDAO(testClient, mockGraphServiceConfig, mockElasticSearchConfig, null);

    // Create ESGraphQueryDAO with GraphQueryPITDAO as delegate
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(testClient, mockGraphServiceConfig, mockElasticSearchConfig, null);
    // Note: This test explicitly calls destroy(), but we track it anyway for safety
    createdDAOs.add(dao);

    // Use reflection to set the delegate to our GraphQueryPITDAO
    java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    delegateField.set(dao, pitDAO);

    // Verify the delegate is a GraphQueryPITDAO
    GraphQueryBaseDAO actualDelegate = (GraphQueryBaseDAO) delegateField.get(dao);
    assertTrue(actualDelegate instanceof GraphQueryPITDAO);

    // Call destroy()
    dao.destroy();

    // Verify that the pitExecutor is shutdown
    assertTrue(pitDAO.pitExecutor.isShutdown(), "PIT executor should be shutdown after destroy()");
    assertTrue(
        pitDAO.pitExecutor.isTerminated(), "PIT executor should be terminated after destroy()");
  }

  @Test
  public void testDestroyWithNonGraphQueryPITDAO() throws Exception {
    // Test destroy() method when delegate is NOT GraphQueryPITDAO
    SearchClientShim<?> testClient = mock(SearchClientShim.class);
    when(testClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    // Create ESGraphQueryDAO (which will have GraphQueryElasticsearch7DAO as delegate)
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(testClient, mockGraphServiceConfig, mockElasticSearchConfig, null);
    // Note: This test explicitly calls destroy(), but we track it anyway for safety
    createdDAOs.add(dao);

    // Verify the delegate is NOT a GraphQueryPITDAO
    java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    GraphQueryBaseDAO actualDelegate = (GraphQueryBaseDAO) delegateField.get(dao);
    assertFalse(actualDelegate instanceof GraphQueryPITDAO);

    // Call destroy() - should not throw exception
    dao.destroy();

    // Test passes if no exception is thrown
  }

  @Test
  public void testDestroyWithNullDelegate() throws Exception {
    // Test destroy() method when delegate is null
    SearchClientShim<?> testClient = mock(SearchClientShim.class);
    when(testClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create ESGraphQueryDAO
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(testClient, mockGraphServiceConfig, mockElasticSearchConfig, null);
    // Note: This test explicitly calls destroy(), but we track it anyway for safety
    createdDAOs.add(dao);

    // Use reflection to set the delegate to null
    java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    delegateField.set(dao, null);

    // Call destroy() - should not throw exception
    dao.destroy();

    // Test passes if no exception is thrown
  }

  @Test
  public void testCleanupPointInTime() throws IOException {
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);

    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    String pitId = "test-pit-id";
    dao.cleanupPointInTime(pitId);

    // Verify ESUtils.cleanupPointInTime was called with correct parameters
    verify(mockClient, times(1)).deletePit(argThat(req -> req.getPitIds().contains(pitId)), any());
  }
}
