package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A count-only search keeps the lexical query and facets and does not hybrid-rerank. */
public class ESSearchDAOCountOnlyTest {
  private SearchClientShim<?> client;
  private HybridSearchResultReranker reranker;
  private OperationContext opContext;
  private ESSearchDAO dao;

  @BeforeMethod
  public void setup() {
    client = mock(SearchClientShim.class);
    reranker = mock(HybridSearchResultReranker.class);
    when(reranker.vectorEntityNames(any(), any()))
        .thenReturn(Set.of(Constants.DATASET_ENTITY_NAME));
    opContext =
        TestOperationContexts.withFixedSearchClient(
                TestOperationContexts.systemContextNoSearchAuthorization(), client)
            .withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setGetSuggestions(true)
                        .setSkipAggregates(false)
                        .setIncludeDefaultFacets(true));
    dao =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            reranker);
  }

  @Test
  public void testCountOnlyDoesNotExpandOrRerank() throws Exception {
    stubEmptyResponse();
    dao.search(
        opContext,
        List.of(Constants.DATASET_ENTITY_NAME),
        "customer orders",
        null,
        null,
        0,
        0,
        List.of());

    SearchRequest request = capturedRequest();
    assertEquals(request.source().size(), 0);
    assertTrue(request.source().query().toString().contains("customer orders"));
    assertNotNull(request.source().aggregations());
    assertNull(request.source().suggest());
    verify(reranker, never()).vectorEntityNames(any(), any());
  }

  @Test
  public void testHitSearchStillFetchesTheHybridWindow() throws Exception {
    stubEmptyResponse();
    dao.search(
        opContext,
        List.of(Constants.DATASET_ENTITY_NAME),
        "customer orders",
        null,
        null,
        0,
        10,
        List.of());

    ArgumentCaptor<SearchRequest> requests = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, atLeastOnce())
        .search(any(OperationContext.class), requests.capture(), eq(RequestOptions.DEFAULT));
    assertTrue(
        requests.getAllValues().stream().allMatch(request -> request.source().size() == 100));
    verify(reranker, atLeastOnce()).vectorEntityNames(any(), any());
  }

  private void stubEmptyResponse() throws Exception {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    when(response.getHits()).thenReturn(hits);
    when(hits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(hits.getHits()).thenReturn(new SearchHit[0]);
    when(client.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
  }

  private SearchRequest capturedRequest() throws Exception {
    ArgumentCaptor<SearchRequest> requests = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client)
        .search(any(OperationContext.class), requests.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(requests.getAllValues().size(), 1);
    return requests.getValue();
  }
}
