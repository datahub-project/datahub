package com.linkedin.metadata.dao.search;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.testing.EntityDocument;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static com.linkedin.metadata.utils.TestUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;
import static org.mockito.Mockito.*;


public class ESSearchDAOTest {

  private ESSearchDAO<EntityDocument> _searchDAO;
  private ESAutoCompleteQueryForHighCardinalityFields _esAutoCompleteQuery;
  private TestSearchConfig _testSearchConfig;

  @BeforeMethod
  public void setup() throws Exception {
    _testSearchConfig = new TestSearchConfig();
    _searchDAO = new ESSearchDAO(null, EntityDocument.class, _testSearchConfig);
    _esAutoCompleteQuery = new ESAutoCompleteQueryForHighCardinalityFields(_testSearchConfig);
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
  public void testGetSuggestionList() throws Exception {
    SearchHits searchHits = mock(SearchHits.class);
    SearchHit hit1 = makeSearchHit(1);
    SearchHit hit2 = makeSearchHit(2);
    SearchHit hit3 = makeSearchHit(3);
    when(searchHits.getHits()).thenReturn(new SearchHit[]{hit1, hit2, hit3});
    when(searchHits.getTotalHits()).thenReturn(10L);
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(searchHits);

    StringArray res  = _esAutoCompleteQuery.getSuggestionList(searchResponse, "name", "test", 2);

    assertEquals(res.size(), 2);
  }

  @Test
  public void testExtractSearchResultMetadata() throws Exception {
    // Test: no aggregations in search response
    SearchHits searchHits1 = mock(SearchHits.class);
    when(searchHits1.getTotalHits()).thenReturn(10L);
    SearchResponse searchResponse1 = mock(SearchResponse.class);
    when(searchResponse1.getHits()).thenReturn(searchHits1);
    assertEquals(_searchDAO.extractSearchResultMetadata(searchResponse1), getDefaultSearchResultMetadata());

    // Test: urn field exists in search document
    SearchHits searchHits2 = mock(SearchHits.class);
    SearchHit hit1 = makeSearchHit(1);
    SearchHit hit2 = makeSearchHit(2);
    when(searchHits2.getHits()).thenReturn(new SearchHit[]{hit1, hit2});
    SearchResponse searchResponse2 = mock(SearchResponse.class);
    when(searchResponse2.getHits()).thenReturn(searchHits2);
    UrnArray urns = new UrnArray(Arrays.asList(makeUrn(1), makeUrn(2)));
    assertEquals(_searchDAO.extractSearchResultMetadata(searchResponse2), getDefaultSearchResultMetadata().setUrns(urns));

    // Test: urn field does not exist in one search document, exists in another
    SearchHits searchHits3 = mock(SearchHits.class);
    SearchHit hit3 = mock(SearchHit.class);
    when(hit3.getField("urn")).thenReturn(null);
    SearchHit hit4 = makeSearchHit(1);
    when(searchHits3.getHits()).thenReturn(new SearchHit[]{hit3, hit4});
    SearchResponse searchResponse3 = mock(SearchResponse.class);
    when(searchResponse3.getHits()).thenReturn(searchHits3);
    assertThrows(RuntimeException.class, () -> _searchDAO.extractSearchResultMetadata(searchResponse3));
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
    assertEquals(_searchDAO.buildDocumentsDataMap(sourceData), dataMap);
  }

  @Test
  public void testFilteredQueryWithTermsFilter() throws IOException {
    int from = 0;
    int size = 10;
    Filter filter = newFilter(ImmutableMap.of("key1", "value1, value2 ", "key2", "value3", "key3", " "));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    // Test 1: sort order provided
    SearchRequest searchRequest = _searchDAO.getFilteredSearchQuery(filter, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("SortByUrnTermsFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[] {_testSearchConfig.getIndexName()});

    // Test 2: no sort order provided, default is used.
    searchRequest = _searchDAO.getFilteredSearchQuery(filter, null, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("DefaultSortTermsFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[] {_testSearchConfig.getIndexName()});

    // Test 3: empty request map provided
    searchRequest = _searchDAO.getFilteredSearchQuery(EMPTY_FILTER, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("EmptyFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[] {_testSearchConfig.getIndexName()});
  }

  @Test
  public void testFilteredQueryWithRangeFilter() throws IOException {
    int from = 0;
    int size = 10;
    final Filter filter1 = new Filter().setCriteria(new CriterionArray(Arrays.asList(
        new Criterion().setField("field_gt").setValue("100").setCondition(Condition.GREATER_THAN),
        new Criterion().setField("field_gte").setValue("200").setCondition(Condition.GREATER_THAN_OR_EQUAL_TO),
        new Criterion().setField("field_lt").setValue("300").setCondition(Condition.LESS_THAN),
        new Criterion().setField("field_lte").setValue("400").setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
    )));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    SearchRequest searchRequest = _searchDAO.getFilteredSearchQuery(filter1, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("RangeFilterQuery.json"));
    assertEquals(searchRequest.indices(), new String[] {_testSearchConfig.getIndexName()});
  }

  @Test
  public void testFilteredQueryUnsupportedCondition() {
    int from = 0;
    int size = 10;
    final Filter filter2 = new Filter().setCriteria(new CriterionArray(Arrays.asList(
        new Criterion().setField("field_contain").setValue("value_contain").setCondition(Condition.CONTAIN)
    )));
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");
    assertThrows(UnsupportedOperationException.class, () -> _searchDAO.getFilteredSearchQuery(filter2, sortCriterion, from, size));
  }

  private static SearchHit makeSearchHit(int id) {
    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", makeUrn(id).toString());
    sourceMap.put("name", "test" + id);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    when(hit.getSource()).thenReturn(sourceMap);
    return hit;
  }

  private static SearchResultMetadata getDefaultSearchResultMetadata() {
    return new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());
  }
}
