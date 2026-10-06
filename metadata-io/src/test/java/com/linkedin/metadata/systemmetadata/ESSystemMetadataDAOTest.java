package com.linkedin.metadata.systemmetadata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSystemMetadataDAOTest {

  private static final IndexConvention TEST_INDEX_CONVENTION =
      IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

  private SearchClientShim<?> mockClient;
  private ESBulkProcessor mockBulkProcessor;
  private ESSystemMetadataDAO dao;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    mockClient = mock(SearchClientShim.class);
    mockBulkProcessor = mock(ESBulkProcessor.class);
    SystemMetadataServiceConfig config = mock(SystemMetadataServiceConfig.class);
    dao = new ESSystemMetadataDAO(mockClient, TEST_INDEX_CONVENTION, mockBulkProcessor, 0, config);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testUpsertDocumentRoutesByDocId() {
    String docId = "sysmeta-doc-abc";
    String document = "{\"urn\":\"urn:li:dataset:foo\",\"aspect\":\"ownership\",\"removed\":false}";

    dao.upsertDocument(opContext, docId, document);

    ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(eq(opContext), eq(docId), captor.capture());
    UpdateRequest captured = captor.getValue();
    assertNotNull(captured);
    assertEquals(captured.id(), docId);
  }

  @Test
  public void testUpsertDocumentUsesRoutingOverload() {
    dao.upsertDocument(opContext, "doc-1", "{}");
    verify(mockBulkProcessor)
        .add(any(OperationContext.class), any(String.class), any(UpdateRequest.class));
  }

  @Test
  public void countByKeyAspectParsesActiveAndSoftDeletedCounts() throws IOException {
    SearchResponse response = buildFiltersSearchResponse(7L, 3L);
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    KeyAspectCount count = dao.countByKeyAspect(opContext, "datasetKey");

    assertEquals(count.getActiveCount(), 7L);
    assertEquals(count.getSoftDeletedCount(), 3L);
  }

  @Test
  public void countByKeyAspectReturnsEmptyWhenAggregationsMissing() throws IOException {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(null);
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    KeyAspectCount count = dao.countByKeyAspect(opContext, "datasetKey");

    assertEquals(count.getActiveCount(), 0L);
    assertEquals(count.getSoftDeletedCount(), 0L);
  }

  @Test
  public void countByKeyAspectReturnsEmptyWhenRemovalStatusAggregationMissing() throws IOException {
    Aggregations aggregations = mock(Aggregations.class);
    doReturn(null).when(aggregations).get(ESSystemMetadataDAO.AGG_BY_REMOVAL_STATUS);
    SearchResponse response = mock(SearchResponse.class);
    doReturn(aggregations).when(response).getAggregations();
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    KeyAspectCount count = dao.countByKeyAspect(opContext, "datasetKey");

    assertEquals(count.getActiveCount(), 0L);
    assertEquals(count.getSoftDeletedCount(), 0L);
  }

  @Test
  public void countByKeyAspectsBatchReturnsEmptyCountsWhenAggregationsMissing() throws IOException {
    SearchResponse response = mock(SearchResponse.class);
    doReturn(null).when(response).getAggregations();
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    Map<String, KeyAspectCount> counts =
        dao.countByKeyAspects(opContext, List.of("chartKey", "datasetKey"));

    assertEquals(counts.get("chartKey").getActiveCount(), 0L);
    assertEquals(counts.get("datasetKey").getActiveCount(), 0L);
  }

  @Test
  public void countByKeyAspectsBatchReturnsEmptyCountsWhenTermsAggregationMissing()
      throws IOException {
    Aggregations aggregations = mock(Aggregations.class);
    doReturn(null).when(aggregations).get(ESSystemMetadataDAO.AGG_BY_KEY_ASPECT);
    SearchResponse response = mock(SearchResponse.class);
    doReturn(aggregations).when(response).getAggregations();
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    Map<String, KeyAspectCount> counts =
        dao.countByKeyAspects(opContext, List.of("chartKey", "datasetKey"));

    assertEquals(counts.get("chartKey").getActiveCount(), 0L);
    assertEquals(counts.get("datasetKey").getActiveCount(), 0L);
  }

  @Test
  public void countByKeyAspectWrapsIOException() throws IOException {
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenThrow(new IOException("search failed"));

    expectThrows(RuntimeException.class, () -> dao.countByKeyAspect(opContext, "datasetKey"));
  }

  @Test
  public void countByKeyAspectsEmptyListReturnsEmptyMap() {
    assertTrue(dao.countByKeyAspects(opContext, List.of()).isEmpty());
  }

  @Test
  public void countByKeyAspectsSingleAspectUsesFiltersAggregation() throws IOException {
    SearchResponse response = buildFiltersSearchResponse(4L, 1L);
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    Map<String, KeyAspectCount> counts = dao.countByKeyAspects(opContext, List.of("chartKey"));

    assertEquals(counts.get("chartKey").getActiveCount(), 4L);
    assertEquals(counts.get("chartKey").getSoftDeletedCount(), 1L);
  }

  @Test
  public void countByKeyAspectsBatchParsesTermsAggregation() throws IOException {
    SearchResponse response = buildTermsSearchResponse(Map.of("chartKey", new long[] {2L, 1L}));
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenReturn(response);

    Map<String, KeyAspectCount> counts =
        dao.countByKeyAspects(opContext, List.of("chartKey", "datasetKey"));

    assertEquals(counts.get("chartKey").getActiveCount(), 2L);
    assertEquals(counts.get("chartKey").getSoftDeletedCount(), 1L);
    assertEquals(counts.get("datasetKey").getActiveCount(), 0L);
    assertEquals(counts.get("datasetKey").getSoftDeletedCount(), 0L);
  }

  @Test
  public void countByKeyAspectsBatchWrapsIOException() throws IOException {
    when(mockClient.search(any(OperationContext.class), any(SearchRequest.class), any()))
        .thenThrow(new IOException("batch search failed"));

    expectThrows(
        RuntimeException.class,
        () -> dao.countByKeyAspects(opContext, List.of("chartKey", "datasetKey")));
  }

  private static SearchResponse buildFiltersSearchResponse(long active, long softDeleted) {
    Filters.Bucket activeBucket = mock(Filters.Bucket.class);
    doReturn(ESSystemMetadataDAO.FILTER_ACTIVE).when(activeBucket).getKeyAsString();
    doReturn(active).when(activeBucket).getDocCount();

    Filters.Bucket softDeletedBucket = mock(Filters.Bucket.class);
    doReturn(ESSystemMetadataDAO.FILTER_SOFT_DELETED).when(softDeletedBucket).getKeyAsString();
    doReturn(softDeleted).when(softDeletedBucket).getDocCount();

    ParsedFilters parsedFilters = mock(ParsedFilters.class);
    doReturn(List.of(activeBucket, softDeletedBucket)).when(parsedFilters).getBuckets();

    Aggregations aggregations = mock(Aggregations.class);
    doReturn(parsedFilters).when(aggregations).get(ESSystemMetadataDAO.AGG_BY_REMOVAL_STATUS);

    SearchResponse response = mock(SearchResponse.class);
    doReturn(aggregations).when(response).getAggregations();
    return response;
  }

  private static SearchResponse buildTermsSearchResponse(Map<String, long[]> countsByAspect) {
    ParsedStringTerms parsedTerms = mock(ParsedStringTerms.class);
    List<Terms.Bucket> buckets =
        countsByAspect.entrySet().stream()
            .map(
                entry ->
                    buildAspectBucket(entry.getKey(), entry.getValue()[0], entry.getValue()[1]))
            .toList();
    doReturn(buckets).when(parsedTerms).getBuckets();

    Aggregations aggregations = mock(Aggregations.class);
    doReturn(parsedTerms).when(aggregations).get(ESSystemMetadataDAO.AGG_BY_KEY_ASPECT);

    SearchResponse response = mock(SearchResponse.class);
    doReturn(aggregations).when(response).getAggregations();
    return response;
  }

  private static Terms.Bucket buildAspectBucket(String aspect, long active, long softDeleted) {
    Terms.Bucket bucket = mock(Terms.Bucket.class);
    doReturn(aspect).when(bucket).getKeyAsString();

    ParsedFilter activeAgg = mock(ParsedFilter.class);
    doReturn(active).when(activeAgg).getDocCount();
    ParsedFilter softDeletedAgg = mock(ParsedFilter.class);
    doReturn(softDeleted).when(softDeletedAgg).getDocCount();

    Aggregations bucketAggs = mock(Aggregations.class);
    doReturn(activeAgg).when(bucketAggs).get(ESSystemMetadataDAO.AGG_ACTIVE);
    doReturn(softDeletedAgg).when(bucketAggs).get(ESSystemMetadataDAO.AGG_SOFT_DELETED);
    doReturn(bucketAggs).when(bucket).getAggregations();
    return bucket;
  }
}
