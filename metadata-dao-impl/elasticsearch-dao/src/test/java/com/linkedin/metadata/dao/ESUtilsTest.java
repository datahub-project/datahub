package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.dao.utils.ESUtils;
import java.util.Collections;
import java.util.Map;
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
  public void testEscapeReservedCharacters() {
    assertEquals(escapeReservedCharacters("foobar"), "foobar");
    assertEquals(escapeReservedCharacters("**"), "\\*\\*");
    assertEquals(escapeReservedCharacters("()"), "\\(\\)");
    assertEquals(escapeReservedCharacters("{}"), "\\{\\}");
  }
}
