package com.linkedin.metadata.search.query;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
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
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class BrowseDAOTest extends AbstractTestNGSpringContextTests {
  private RestHighLevelClient _mockClient;
  private ESBrowseDAO _browseDAO;

  @Autowired private SearchConfiguration _searchConfiguration;
  @Autowired private CustomSearchConfiguration _customSearchConfiguration;

  @BeforeMethod
  public void setup() {
    _mockClient = mock(RestHighLevelClient.class);
    _browseDAO =
        new ESBrowseDAO(
            new TestEntityRegistry(),
            _mockClient,
            new IndexConventionImpl("es_browse_dao_test"),
            _searchConfiguration,
            _customSearchConfiguration);
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
    when(_mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths("dataset", dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field doesn't exist
    sourceMap.remove("browse_paths");
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths("dataset", dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field exists
    sourceMap.put("browsePaths", Collections.singletonList("foo"));
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    List<String> browsePaths = _browseDAO.getBrowsePaths("dataset", dummyUrn);
    assertEquals(browsePaths.size(), 1);
    assertEquals(browsePaths.get(0), "foo");
  }
}
