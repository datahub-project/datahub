package com.linkedin.metadata.dao.search;

import com.linkedin.common.UrnArray;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.testing.EntityDocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;
import static org.mockito.Mockito.*;


public class ESSearchDAOTest {

  private ESSearchDAO<EntityDocument> _mockSearchDAO;

  @BeforeMethod
  public void setup() throws Exception {
    _mockSearchDAO = new ESSearchDAO(null, EntityDocument.class, new TestSearchConfig());
  }

  @Test
  public void testDecoupleArrayToGetSubstringMatch() throws Exception {
    // Test empty fieldVal
    List<String> fieldValList = Collections.emptyList();
    String searchInput = "searchInput";
    List<String> searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test non-list fieldVal
    String fieldValString = "fieldVal";
    searchInput = "searchInput";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValString, searchInput);
    assertEquals(searchResult.size(), 1);

    // Test list fieldVal with no match
    fieldValList = Arrays.asList("fieldVal1", "fieldVal2", "fieldVal3");
    searchInput = "searchInput";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test list fieldVal with single match
    searchInput = "val1";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 1);
    assertEquals(searchResult.get(0), "fieldVal1");

    // Test list fieldVal with multiple match
    searchInput = "val";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 3);
    assertTrue(searchResult.equals(fieldValList));
  }

  @Test
  public void testExtractSearchResultMetadata() throws Exception {
    // Test: no aggregations in search response
    SearchHits searchHits1 = mock(SearchHits.class);
    when(searchHits1.getTotalHits()).thenReturn(10L);
    SearchResponse searchResponse1 = mock(SearchResponse.class);
    when(searchResponse1.getHits()).thenReturn(searchHits1);
    assertEquals(_mockSearchDAO.extractSearchResultMetadata(searchResponse1), getDefaultSearchResultMetadata());

    // Test: urn field exists in search document
    SearchHits searchHits2 = mock(SearchHits.class);
    SearchHit hit1 = makeSearchHit(1);
    SearchHit hit2 = makeSearchHit(2);
    when(searchHits2.getHits()).thenReturn(new SearchHit[]{hit1, hit2});
    SearchResponse searchResponse2 = mock(SearchResponse.class);
    when(searchResponse2.getHits()).thenReturn(searchHits2);
    UrnArray urns = new UrnArray(Arrays.asList(makeUrn(1), makeUrn(2)));
    assertEquals(_mockSearchDAO.extractSearchResultMetadata(searchResponse2), getDefaultSearchResultMetadata().setUrns(urns));

    // Test: urn field does not exist in one search document, exists in another
    SearchHits searchHits3 = mock(SearchHits.class);
    SearchHit hit3 = mock(SearchHit.class);
    when(hit3.getField("urn")).thenReturn(null);
    SearchHit hit4 = makeSearchHit(1);
    when(searchHits3.getHits()).thenReturn(new SearchHit[]{hit3, hit4});
    SearchResponse searchResponse3 = mock(SearchResponse.class);
    when(searchResponse3.getHits()).thenReturn(searchHits3);
    assertThrows(RuntimeException.class, () -> _mockSearchDAO.extractSearchResultMetadata(searchResponse3));
  }

  @Test
  public void testBuildDocumentsDataMap() {
    Map<String, Object> sourceData = new HashMap<>();
    sourceData.put("field1", "val1");
    sourceData.put("field2", null);
    ArrayList<String> arrayList = new ArrayList<>(Arrays.asList("foo", "bar"));
    sourceData.put("field3", arrayList);
    DataMap dataMap = new DataMap();
    dataMap.put("field1", "val1");
    dataMap.put("field3", new DataList(arrayList));
    assertEquals(_mockSearchDAO.buildDocumentsDataMap(sourceData), dataMap);
  }

  private static SearchHit makeSearchHit(int id) {
    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", makeUrn(id).toString());
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    return hit;
  }

  private static SearchResultMetadata getDefaultSearchResultMetadata() {
    return new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());
  }
}
