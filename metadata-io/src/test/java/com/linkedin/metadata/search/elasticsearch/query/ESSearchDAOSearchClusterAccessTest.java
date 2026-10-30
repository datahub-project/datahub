package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.SearchOperation;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.core.CountResponse;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

public class ESSearchDAOSearchClusterAccessTest {

  @Test
  public void testKeywordReadV3UsesV3ClientNotPrimary() throws Exception {
    SearchClientShim<?> primary = mock(SearchClientShim.class);
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(4L);
    when(v3.count(any(), any(), any())).thenReturn(countResponse);

    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;

    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    ESSearchDAO dao =
        new ESSearchDAO(
            false,
            v3KeywordReadConfig(),
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);

    assertEquals(dao.docCount(opContext, "dataset"), 4L);
    verify(v3).count(any(), any(), any());
    verify(v2, never()).count(any(), any(), any());
    verify(primary, never()).count(any(), any(), any());
  }

  @Test
  public void testSearchUsesV3ClientNotPrimary() throws Exception {
    SearchClientShim<?> primary = mock(SearchClientShim.class);
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    SearchResponse searchResponse = emptySearchResponse();
    when(v3.search(any(), any(), any())).thenReturn(searchResponse);

    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    ESSearchDAO dao = dao(primary, false);

    dao.search(opContext, List.of("dataset"), "*", null, null, 0, 10, List.of());

    verify(v3).search(any(), any(), any());
    verify(v2, never()).search(any(), any(), any());
    verify(primary, never()).search(any(), any(), any());
  }

  @Test
  public void testRawUsageIndexUsesUsageClient() throws Exception {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> usage = mock(SearchClientShim.class);
    SearchResponse searchResponse = emptySearchResponse();
    when(usage.search(any(), any(), any())).thenReturn(searchResponse);

    SearchClusterAccess access = component -> component == SearchComponent.USAGE ? usage : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    ESSearchDAO dao = dao(v2, false);
    dao.raw(
        opContext,
        "datahub_usage_event",
        "{\"query\":{\"match_all\":{}}}",
        SearchOperation.DATAHUB_USAGE);

    verify(usage).search(any(), any(), any());
    verify(v2, never()).search(any(), any(), any());
  }

  @Test
  public void testRawUsageSubstringDoesNotUseUsageClient() throws Exception {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> usage = mock(SearchClientShim.class);
    SearchClusterAccess access = component -> component == SearchComponent.USAGE ? usage : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    ESSearchDAO dao = dao(v2, false);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dao.raw(
                opContext,
                "datahub_usage_event_index_template",
                "{\"query\":{\"match_all\":{}}}",
                SearchOperation.DATAHUB_USAGE));
    verify(usage, never()).search(any(), any(), any());
  }

  @Test
  public void testPitScrollUsesV3ClientNotPrimary() throws Exception {
    SearchClientShim<?> primary = mock(SearchClientShim.class);
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    when(v3.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    CreatePitResponse pitResponse = mock(CreatePitResponse.class);
    when(pitResponse.getId()).thenReturn("pit-1");
    when(v3.createPit(any(), any(), any())).thenReturn(pitResponse);
    SearchResponse searchResponse = emptySearchResponse();
    when(v3.search(any(), any(), any())).thenReturn(searchResponse);

    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    ESSearchDAO dao = dao(primary, true);

    dao.scroll(opContext, List.of("dataset"), "*", null, null, null, "1m", 10);

    verify(v3).createPit(any(), any(), any());
    verify(v3).search(any(), any(), any());
    verify(v2, never()).createPit(any(), any(), any());
    verify(v2, never()).search(any(), any(), any());
    verify(primary, never()).createPit(any(), any(), any());
    verify(primary, never()).search(any(), any(), any());
  }

  @Test
  public void testLastPitScrollPageClosesPointInTime() throws Exception {
    SearchClientShim<?> v3 = pitClient(searchResponse("pit-1", new SearchHit[0]));
    ESSearchDAO dao = dao(v3, false);

    ScrollResult page =
        dao.scroll(v3Context(v3), List.of("dataset"), "*", null, null, null, "1m", 10);

    assertNull(page.getScrollId());
    ArgumentCaptor<DeletePitRequest> deleted = ArgumentCaptor.forClass(DeletePitRequest.class);
    verify(v3).deletePit(any(), deleted.capture(), any());
    assertEquals(deleted.getValue().getPitIds(), List.of("pit-1"));
  }

  @Test
  public void testNonLastPitScrollPageKeepsPointInTimeOpen() throws Exception {
    SearchHit hit = new SearchHit(1);
    hit.sortValues(new Object[] {"urn:li:corpuser:a"}, new DocValueFormat[] {DocValueFormat.RAW});
    SearchClientShim<?> v3 = pitClient(searchResponse("pit-1", new SearchHit[] {hit}));
    ESSearchDAO dao = dao(v3, false);

    ScrollResult page =
        dao.scroll(v3Context(v3), List.of("dataset"), "*", null, null, null, "1m", 1);

    assertNotNull(page.getScrollId());
    verify(v3, never()).deletePit(any(), any(), any());
  }

  private static SearchClientShim<?> pitClient(SearchResponse searchResponse) throws Exception {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    CreatePitResponse pitResponse = mock(CreatePitResponse.class);
    when(pitResponse.getId()).thenReturn("pit-1");
    when(client.createPit(any(), any(), any())).thenReturn(pitResponse);
    when(client.search(any(), any(), any())).thenReturn(searchResponse);
    return client;
  }

  private static OperationContext v3Context(SearchClientShim<?> v3) {
    return TestOperationContexts.withSearchClusterAccess(
        TestOperationContexts.systemContextNoSearchAuthorization(), component -> v3);
  }

  private static SearchResponse searchResponse(String pitId, SearchHit[] searchHits) {
    SearchHits hits = mock(SearchHits.class);
    when(hits.getHits()).thenReturn(searchHits);
    when(hits.getTotalHits())
        .thenReturn(new TotalHits(searchHits.length, TotalHits.Relation.EQUAL_TO));
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits()).thenReturn(hits);
    when(response.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
    when(response.pointInTimeId()).thenReturn(pitId);
    return response;
  }

  private static SearchResponse emptySearchResponse() {
    SearchHits hits = mock(SearchHits.class);
    when(hits.getHits()).thenReturn(new SearchHit[0]);
    when(hits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits()).thenReturn(hits);
    when(response.getAggregations()).thenReturn(null);
    when(response.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
    when(response.getSuggest()).thenReturn(null);
    return response;
  }

  private static ESSearchDAO dao(SearchClientShim<?> primary, boolean pitEnabled) {
    return new ESSearchDAO(
        pitEnabled,
        v3KeywordReadConfig(),
        null,
        QueryFilterRewriteChain.EMPTY,
        TEST_SEARCH_SERVICE_CONFIG);
  }

  private static ElasticSearchConfiguration v3KeywordReadConfig() {
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .build())
                .build())
        .build();
  }
}
