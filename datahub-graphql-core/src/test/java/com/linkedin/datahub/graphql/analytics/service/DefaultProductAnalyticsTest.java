package com.linkedin.datahub.graphql.analytics.service;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultProductAnalyticsTest {

  private SearchClientShim<?> mockElasticClient;
  private IndexConvention mockIndexConvention;

  @BeforeMethod
  public void setUp() {
    mockElasticClient = mock(SearchClientShim.class);
    mockIndexConvention = mock(IndexConvention.class);
    when(mockIndexConvention.getEntityIndexName(anyString())).thenReturn("corpuserindex_v2");
  }

  private DefaultProductAnalytics newAnalytics() {
    return new DefaultProductAnalytics(mockElasticClient, mockIndexConvention, null);
  }

  @Test
  public void testCountCorpUsersTotalHandlesSearchError() throws Exception {
    when(mockElasticClient.search(
            any(OperationFingerprint.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenThrow(new RuntimeException("Search failed"));

    assertEquals(newAnalytics().countCorpUsersTotal(), 0);
  }

  @Test
  public void testCountCorpUserServiceAccountsHandlesSearchError() throws Exception {
    when(mockElasticClient.search(
            any(OperationFingerprint.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenThrow(new RuntimeException("Search failed"));

    assertEquals(newAnalytics().countCorpUserServiceAccounts(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCountCorpUsersTotalReturnsCorrectCount() throws Exception {
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    org.apache.lucene.search.TotalHits mockTotalHits =
        new org.apache.lucene.search.TotalHits(
            42, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);

    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockSearchHits.getTotalHits()).thenReturn(mockTotalHits);
    when(mockElasticClient.search(
            any(OperationFingerprint.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    assertEquals(newAnalytics().countCorpUsersTotal(), 42);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCountCorpUserServiceAccountsReturnsCorrectCount() throws Exception {
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    org.apache.lucene.search.TotalHits mockTotalHits =
        new org.apache.lucene.search.TotalHits(
            5, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);

    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockSearchHits.getTotalHits()).thenReturn(mockTotalHits);
    when(mockElasticClient.search(
            any(OperationFingerprint.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    assertEquals(newAnalytics().countCorpUserServiceAccounts(), 5);
  }

  @Test
  public void testCountsReturnZeroWhenElasticDisabled() {
    DefaultProductAnalytics analytics =
        new DefaultProductAnalytics(null, mockIndexConvention, null);
    assertEquals(analytics.countCorpUsersTotal(), 0);
    assertEquals(analytics.countCorpUserServiceAccounts(), 0);
    assertFalse(analytics.isEntitySearchAvailable());
  }
}
