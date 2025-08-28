package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RestHighLevelClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESGraphQueryDAOTest {

  private RestHighLevelClient mockClient;
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

  @BeforeMethod
  public void setUp() {
    mockClient = mock(RestHighLevelClient.class);
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
  }

  @Test
  public void testConstructorWithElasticsearchImplementation() {
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    assertNotNull(dao);
  }

  @Test
  public void testConstructorWithOpenSearchImplementation() {
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    assertNotNull(dao);
  }

  @Test
  public void testConstructorWithUnsupportedImplementation() {
    when(mockElasticSearchConfig.getImplementation()).thenReturn("unsupported");

    assertThrows(
        NotImplementedException.class,
        () -> {
          new ESGraphQueryDAO(
              mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);
        });
  }

  @Test
  public void testGetLineage() {
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(5, Arrays.asList());
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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(3, Arrays.asList());
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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    assertNotNull(dao);
    // The delegate should be an instance of GraphQueryElasticsearch7DAO
    assertTrue(dao instanceof ESGraphQueryDAO);
  }

  @Test
  public void testConstructorCreatesCorrectDelegateForOpenSearch() {
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    assertNotNull(dao);
    // The delegate should be an instance of GraphQueryOpenSearchDAO
    assertTrue(dao instanceof ESGraphQueryDAO);
  }

  @Test
  public void testGetLineageWithNullCount() {
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

    // Use reflection to set the delegate since it's private
    try {
      java.lang.reflect.Field delegateField = ESGraphQueryDAO.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      delegateField.set(dao, mockDelegate);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LineageResponse expectedResponse = new LineageResponse(5, Arrays.asList());
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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
    when(mockElasticSearchConfig.getImplementation())
        .thenReturn(ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient, mockGraphServiceConfig, mockElasticSearchConfig, mockMetricUtils);

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
}
