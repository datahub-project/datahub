package com.linkedin.metadata.search.elasticsearch.query;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class ESBrowseDAOTest {
  private RestHighLevelClient _mockClient;
  private ESBrowseDAO _browseDAO;

  @BeforeMethod
  public void setup() {
    _mockClient = mock(RestHighLevelClient.class);
    _browseDAO = new ESBrowseDAO(new TestEntityRegistry(), _mockClient, new IndexConventionImpl(null));
  }

  public static Urn makeUrn(Object id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMatchingPaths() {
    List<String> browsePaths =
        Arrays.asList("/all/subscriptions/premium_new_signups_v2/subs_new_bookings", "/certified/lls/subs_new_bookings",
            "/certified/lls/lex/subs_new_bookings", "/certified/lls/consumer/subs_new_bookings", "/subs_new_bookings",
            "/School/Characteristics/General/Embedding/network_standardized_school_embeddings_v3");

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
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[]{mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    assertEquals(_browseDAO.getBrowsePaths("dataset", dummyUrn).size(), 0);

    // Test the case of single search hit & browsePaths field exists
    sourceMap.put("browsePaths", Collections.singletonList("foo"));
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[]{mockSearchHit});
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(_mockClient.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(mockSearchResponse);
    List<String> browsePaths = _browseDAO.getBrowsePaths("dataset", dummyUrn);
    assertEquals(browsePaths.size(), 1);
    assertEquals(browsePaths.get(0), "foo");
  }
}