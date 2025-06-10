package com.linkedin.metadata.search.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_CONFIG;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class BrowseDAOTest extends AbstractTestNGSpringContextTests {
  private RestHighLevelClient mockClient;
  private ESBrowseDAO browseDAO;
  private OperationContext opContext;

  @Autowired
  @Qualifier("defaultTestCustomSearchConfig")
  private CustomSearchConfiguration customSearchConfiguration;

  @BeforeMethod
  public void setup() throws RemoteInvocationException, URISyntaxException {
    mockClient = mock(RestHighLevelClient.class);
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .prefix("es_browse_dao_test")
                    .hashIdAlgo("MD5")
                    .build()));
    browseDAO =
        new ESBrowseDAO(
            mockClient,
            TEST_SEARCH_CONFIG,
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY);
  }

  public static Urn makeUrn(Object id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetBrowsePath() throws Exception {
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    SearchHit mockSearchHit = mock(SearchHit.class);
    Urn dummyUrn = makeUrn(0);
    Map<String, Object> sourceMap = new HashMap<>();

    // Test when there is no search hit for getBrowsePaths
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    assertEquals(browseDAO.getBrowsePaths(opContext, "dataset", dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field doesn't exist
    sourceMap.remove("browse_paths");
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    assertEquals(browseDAO.getBrowsePaths(opContext, "dataset", dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field exists
    sourceMap.put("browsePaths", Collections.singletonList("foo"));
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    List<String> browsePaths = browseDAO.getBrowsePaths(opContext, "dataset", dummyUrn);
    assertEquals(browsePaths.size(), 1);
    assertEquals(browsePaths.get(0), "foo");

    // Test the case of null browsePaths field
    sourceMap.put("browsePaths", Collections.singletonList(null));
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    List<String> nullBrowsePaths = browseDAO.getBrowsePaths(opContext, "dataset", dummyUrn);
    assertEquals(nullBrowsePaths.size(), 0);
  }

  @Test
  public void testBrowseWithLimitedResults() throws Exception {
    // Configure mock response for testing browse method
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    // Configure aggregations for groups response
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);

    // Configure mock response for entities search
    SearchResponse mockEntitiesResponse = mock(SearchResponse.class);
    when(mockEntitiesResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[0],
                new TotalHits(0L, TotalHits.Relation.EQUAL_TO),
                0f,
                null,
                null,
                null));

    // Configure client to return our mock responses
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse)
        .thenReturn(mockEntitiesResponse);

    // Configure search configuration with specific limits
    // Create a new browse DAO with our test configuration
    ESBrowseDAO testBrowseDAO =
        new ESBrowseDAO(
            mockClient,
            TEST_SEARCH_CONFIG.toBuilder()
                .search(
                    TEST_SEARCH_CONFIG.getSearch().toBuilder()
                        .limit(
                            new SearchConfiguration.SearchLimitConfig()
                                .setResults(
                                    new SearchConfiguration.SearchResultsLimit()
                                        .setMax(15)
                                        .setStrict(false)))
                        .build())
                .build(),
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY);

    // Test browse with size that exceeds limit
    int requestedSize = 20;
    BrowseResult result =
        testBrowseDAO.browse(opContext, "dataset", "/test/path", null, 0, requestedSize);

    // Verify the client was called with the correct limited size
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient, times(2)).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    // The second request should be the entities search request with limited size
    List<SearchRequest> capturedRequests = requestCaptor.getAllValues();
    assertEquals(capturedRequests.get(1).source().size(), 15);

    // Result should have the correct page size (the original requested size)
    assertEquals(result.getPageSize(), requestedSize);
  }

  @Test
  public void testBrowseV2WithLimitedResults() throws Exception {
    // Configure mock response for testing browseV2 method
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    // Configure aggregations for groups response
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);

    // Configure client to return our mock response
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse);

    // Configure search configuration with specific limits
    SearchConfiguration testConfig = new SearchConfiguration();
    testConfig.setPartial(new PartialConfiguration());
    testConfig.setWordGram(new WordGramConfiguration());
    testConfig.setExactMatch(new ExactMatchConfiguration());
    testConfig.setLimit(
        new SearchConfiguration.SearchLimitConfig()
            .setResults(new SearchConfiguration.SearchResultsLimit().setMax(25).setStrict(false)));

    // Create a new browse DAO with our test configuration
    ESBrowseDAO testBrowseDAO =
        new ESBrowseDAO(
            mockClient,
            TEST_SEARCH_CONFIG,
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY);

    // Call browseV2 with a count that exceeds the limit
    int requestedCount = 30;
    BrowseResultV2 result =
        testBrowseDAO.browseV2(
            opContext, "dataset", "/test/path", null, "test query", 0, requestedCount);

    // Verify the search request captured by the mock client
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    // This method doesn't directly use the size parameter in the captured request,
    // but we can still verify the page size in the result
    assertEquals(result.getPageSize(), requestedCount);
  }
}
