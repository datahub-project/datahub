package com.linkedin.metadata.search.query;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchConfiguration;
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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
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

  @Autowired private SearchConfiguration searchConfiguration;

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
            searchConfiguration,
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
}
