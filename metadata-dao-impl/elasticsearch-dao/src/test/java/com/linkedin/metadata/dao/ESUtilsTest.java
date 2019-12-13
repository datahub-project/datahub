package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.ESUtils.*;
import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;

public class ESUtilsTest {
  @Test
  public void testBuildFilterQuery() throws Exception {
    // Test empty request map
    Map<String, String> requestMap = Collections.emptyMap();
    BoolQueryBuilder queryBuilder = ESUtils.buildFilterQuery(requestMap);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/EmptyFilterQuery.json"));

    // Test and filters
    requestMap = ImmutableMap.of("key1", "value1", "key2", "value2");
    queryBuilder = ESUtils.buildFilterQuery(requestMap);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/AndFilterQuery.json"));

    // Test or filters
    requestMap = ImmutableMap.of("key1", "value1,value2");
    queryBuilder = ESUtils.buildFilterQuery(requestMap);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/OrFilterQuery.json"));

    // Test complex filter
    requestMap = ImmutableMap.of("key1", "value1,value2", "key2", "value2");
    queryBuilder = ESUtils.buildFilterQuery(requestMap);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/ComplexFilterQuery.json"));
  }

  @Test
  public void testGetFilteredSearchQuery() throws IOException {
    int from = 0;
    int size = 10;
    Map<String, String> requestMap = ImmutableMap.of("key1", "value1, value2 ", "key2", "value3", "key3", " ");
    SortCriterion sortCriterion = new SortCriterion().setOrder(SortOrder.ASCENDING).setField("urn");

    // Test 1: sort order provided
    SearchRequest searchRequest = ESUtils.getFilteredSearchQuery(requestMap, sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("SortByUrnTermsFilterQuery.json"));

    // Test 2: no sort order provided, default is used.
    searchRequest = ESUtils.getFilteredSearchQuery(requestMap, null, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("DefaultSortTermsFilterQuery.json"));

    // Test 3: empty request map provided
    searchRequest = ESUtils.getFilteredSearchQuery(Collections.emptyMap(), sortCriterion, from, size);
    assertEquals(searchRequest.source().toString(), loadJsonFromResource("EmptyFilterQuery.json"));
  }

  @Test
  public void testEscapeReservedCharacters() {
    assertEquals(escapeReservedCharacters("foobar"), "foobar");
    assertEquals(escapeReservedCharacters("**"), "\\*\\*");
  }
}
