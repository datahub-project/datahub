package com.linkedin.metadata.dao.browse;

import com.linkedin.common.urn.Urn;
import com.linkedin.testing.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BrowseDAOTest {
  private BaseBrowseConfig _browseConfig;
  private RestHighLevelClient _mockClient;
  private ESBrowseDAO _browseDAO;

  @BeforeMethod
  public void setup() {
    _browseConfig = new TestBrowseConfig();
    _mockClient = mock(RestHighLevelClient.class);
    _browseDAO = new ESBrowseDAO(_mockClient, _browseConfig);
  }

  @Test
  public void testMatchingPaths() {
    List<String> browsePaths = Arrays.asList("/all/subscriptions/premium_new_signups_v2/subs_new_bookings",
        "/certified/lls/subs_new_bookings",
        "/certified/lls/lex/subs_new_bookings",
        "/certified/lls/consumer/subs_new_bookings",
        "/subs_new_bookings",
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3"
    );

    // Scenario 1: inside /Certified/LLS
    String path1 = "/certified/lls";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path1), "/certified/lls/subs_new_bookings");

    // Scenario 2: inside /Certified/LLS/Consumer
    String path2 = "/certified/lls/consumer";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path2), "/certified/lls/consumer/subs_new_bookings");

    // Scenario 3: inside root directory
    String path3 = "";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path3), "/subs_new_bookings");

    // Scenario 4: inside an incorrect path /foo
    // this situation should ideally not arise for entity browse queries
    String path4 = "/foo";
    assertNull(ESBrowseDAO.getNextLevelPath(browsePaths, path4));

    // Scenario 5: one of the browse paths isn't normalized
    String path5 = "/school/characteristics/general/embedding";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path5),
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3");

    // Scenario 6: current path isn't normalized, which ideally should not be the case
    String path6 = "/School/Characteristics/General/Embedding";
    assertEquals(ESBrowseDAO.getNextLevelPath(browsePaths, path6),
        "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3");
  }

  @Test
  public void testGetBrowsePath() throws Exception {
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    SearchHit mockSearchHit = mock(SearchHit.class);
    Urn dummyUrn = TestUtils.makeUrn(0);
    Map mockSourceMap = mock(Map.class);

    // Test when there is no search hit for getBrowsePaths
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any())).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths(dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field doesn't exist
    when(mockSourceMap.containsKey(_browseConfig.getBrowsePathFieldName())).thenReturn(false);
    when(mockSearchHit.getSourceAsMap()).thenReturn(mockSourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[]{mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any())).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths(dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field exists
    when(mockSourceMap.containsKey(_browseConfig.getBrowsePathFieldName())).thenReturn(true);
    when(mockSourceMap.get(_browseConfig.getBrowsePathFieldName())).thenReturn(Collections.singletonList("foo"));
    when(mockSearchHit.getSourceAsMap()).thenReturn(mockSourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[]{mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any())).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths(dummyUrn).size(), 1);
    assertEquals(_browseDAO.getBrowsePaths(dummyUrn).get(0), "foo");
  }
}
