package com.linkedin.metadata.graph.elastic;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ElasticSearchGraphServiceTest {

  private ElasticSearchGraphService test;
  private ESBulkProcessor mockESBulkProcessor;
  private ESGraphWriteDAO mockWriteDAO;
  private ESGraphQueryDAO mockReadDAO;

  @BeforeTest
  public void beforeTest() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    mockESBulkProcessor = mock(ESBulkProcessor.class);
    mockWriteDAO = mock(ESGraphWriteDAO.class);
    mockReadDAO = mock(ESGraphQueryDAO.class);
    test =
        new ElasticSearchGraphService(
            new LineageRegistry(entityRegistry),
            mockESBulkProcessor,
            IndexConventionImpl.noPrefix("md5"),
            mockWriteDAO,
            mockReadDAO,
            mock(ESIndexBuilder.class),
            "md5");
  }

  @BeforeMethod
  public void beforeMethod() {
    reset(mockESBulkProcessor, mockWriteDAO, mockReadDAO);
  }

  @Test
  public void testSetEdgeStatus() {
    final Urn testUrn = UrnUtils.getUrn("urn:li:container:test");
    for (boolean removed : Set.of(true, false)) {
      test.setEdgeStatus(testUrn, removed, EdgeUrnType.values());

      ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
      ArgumentCaptor<QueryBuilder> queryCaptor = ArgumentCaptor.forClass(QueryBuilder.class);
      verify(mockWriteDAO, times(EdgeUrnType.values().length))
          .updateByQuery(scriptCaptor.capture(), queryCaptor.capture());

      queryCaptor
          .getAllValues()
          .forEach(
              queryBuilder -> {
                BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder;

                // urn targeted
                assertEquals(
                    ((TermQueryBuilder) query.filter().get(0)).value(), testUrn.toString());

                // Expected inverse query
                if (removed) {
                  assertEquals(((TermQueryBuilder) query.should().get(0)).value(), "false");
                  assertTrue(
                      ((ExistsQueryBuilder)
                              ((BoolQueryBuilder) query.should().get(1)).mustNot().get(0))
                          .fieldName()
                          .toLowerCase()
                          .contains("removed"));
                } else {
                  assertEquals(((TermQueryBuilder) query.filter().get(1)).value(), "true");
                }
              });

      // reset for next boolean
      reset(mockWriteDAO);
    }
  }

  @Test
  public void testScrollRelatedEntities() {
    // Mock dependencies
    OperationContext mockOpContext = TestOperationContexts.systemContextNoValidate();
    GraphFilters mockGraphFilters = GraphFilters.ALL;
    List<SortCriterion> mockSortCriteria = Collections.emptyList();

    String scrollId = "test-scroll-id";
    int count = 10;

    // Create mock search response
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    when(mockResponse.getHits()).thenReturn(mockHits);

    // Use reflection to create a TotalHits object with the desired value
    TotalHits totalHits = new TotalHits(15L, TotalHits.Relation.EQUAL_TO);
    when(mockHits.getTotalHits()).thenReturn(totalHits);

    // Setup search hits
    SearchHit[] searchHits = new SearchHit[2];
    searchHits[0] = createMockSearchHit("source1", "dest1", "relationshipType1", null);
    searchHits[1] = createMockSearchHit("source2", "dest2", "relationshipType2", "via1");
    when(mockHits.getHits()).thenReturn(searchHits);

    // Mock read DAO behavior
    when(mockReadDAO.getSearchResponse(any(), any(), any(), any(), anyInt()))
        .thenReturn(mockResponse);

    // Call the method under test
    RelatedEntitiesScrollResult result =
        test.scrollRelatedEntities(
            mockOpContext, mockGraphFilters, mockSortCriteria, scrollId, count, null, null);

    // Verify the DAO was called correctly
    verify(mockReadDAO)
        .getSearchResponse(
            eq(mockOpContext), eq(mockGraphFilters), eq(mockSortCriteria), eq(scrollId), eq(count));

    // Verify result contains expected values
    assertEquals(result.getNumResults(), 15);
    assertEquals(result.getPageSize(), 2);
    assertEquals(result.getEntities().size(), 2);

    // Verify the returned entities
    RelatedEntities entity1 = result.getEntities().get(0);
    assertEquals(entity1.getRelationshipType(), "relationshipType1");
    assertEquals(entity1.getSourceUrn(), "source1");
    assertEquals(entity1.getDestinationUrn(), "dest1");
    assertEquals(entity1.getVia(), null);

    RelatedEntities entity2 = result.getEntities().get(1);
    assertEquals(entity2.getRelationshipType(), "relationshipType2");
    assertEquals(entity2.getSourceUrn(), "source2");
    assertEquals(entity2.getDestinationUrn(), "dest2");
    assertEquals(entity2.getVia(), "via1");

    // Verify scrollId is null since searchHits.length < count
    assertNull(result.getScrollId());
  }

  @Test
  public void testFindRelatedEntitiesNullResponse() {
    // Mock dependencies
    OperationContext mockOpContext = TestOperationContexts.systemContextNoValidate();
    GraphFilters mockGraphFilters = GraphFilters.ALL;
    int offset = 5;
    int count = 10;

    // Mock read DAO behavior to return null
    when(mockReadDAO.getSearchResponse(
            any(OperationContext.class), any(GraphFilters.class), anyInt(), anyInt()))
        .thenReturn(null);

    // Call the method under test
    RelatedEntitiesResult result =
        test.findRelatedEntities(mockOpContext, mockGraphFilters, offset, count);

    // Verify the DAO was called correctly
    verify(mockReadDAO)
        .getSearchResponse(eq(mockOpContext), eq(mockGraphFilters), eq(offset), eq(count));

    // Verify result contains expected values for a null response
    assertEquals(result.getStart(), offset);
    assertEquals(result.getCount(), 0);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getEntities().isEmpty());
  }

  @Test
  public void testFindRelatedEntitiesNoResultsByType() {
    // Mock dependencies
    OperationContext mockOpContext = TestOperationContexts.systemContextNoValidate();
    GraphFilters mockGraphFilters = mock(GraphFilters.class);
    int offset = 5;
    int count = 10;

    // Configure graphFilters to return true for noResultsByType
    when(mockGraphFilters.noResultsByType()).thenReturn(true);

    // Call the method under test
    RelatedEntitiesResult result =
        test.findRelatedEntities(mockOpContext, mockGraphFilters, offset, count);

    // Verify that noResultsByType was called
    verify(mockGraphFilters).noResultsByType();

    // Verify that the search response method was NOT called since we short-circuit
    verify(mockReadDAO, never()).getSearchResponse(any(), any(), anyInt(), anyInt());

    // Verify result contains expected values for noResultsByType
    assertEquals(result.getStart(), offset);
    assertEquals(result.getCount(), 0);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getEntities().isEmpty());
  }

  // Helper method to create mock search hits
  private SearchHit createMockSearchHit(
      String sourceUrn, String destUrn, String relType, String via) {
    SearchHit mockHit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();

    Map<String, String> sourceObj = new HashMap<>();
    sourceObj.put("urn", sourceUrn);
    sourceMap.put("source", sourceObj);

    Map<String, String> destObj = new HashMap<>();
    destObj.put("urn", destUrn);
    sourceMap.put("destination", destObj);

    sourceMap.put("relationshipType", relType);

    if (via != null) {
      sourceMap.put("via", via);
    }

    when(mockHit.getSourceAsMap()).thenReturn(sourceMap);
    return mockHit;
  }
}
