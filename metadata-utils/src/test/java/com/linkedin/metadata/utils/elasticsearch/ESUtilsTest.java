package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.elasticsearch.ESUtils.*;
import static org.testng.Assert.*;


public class ESUtilsTest {

  private static String loadJsonFromResource(String resourceName) throws IOException {
    return IOUtils.toString(ClassLoader.getSystemResourceAsStream(resourceName), StandardCharsets.UTF_8);
  }

  @Test
  public void testBuildFilterQueryWithEmptyFilter() throws Exception {
    // Test null filter
    BoolQueryBuilder queryBuilder = buildFilterQuery(null);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/EmptyFilterQuery.json"));

    // Test empty filter
    Filter filter = new Filter().setCriteria(new CriterionArray());
    queryBuilder = buildFilterQuery(filter);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/EmptyFilterQuery.json"));
  }

  @Test
  public void testBuildFilterQueryWithAndFilter() throws IOException {
    Filter filter = new Filter().setCriteria(new CriterionArray(
        Arrays.asList(new Criterion().setField("key1").setValue("value1").setCondition(Condition.EQUAL),
            new Criterion().setField("key2").setValue("value2").setCondition(Condition.EQUAL))));
    QueryBuilder queryBuilder = buildFilterQuery(filter);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/AndFilterQuery.json"));
  }

  @Test
  public void testBuildFilterQueryWithComplexFilter() throws IOException {
    Filter filter = new Filter().setCriteria(new CriterionArray(
        Arrays.asList(new Criterion().setField("key1").setValue("value1,value2").setCondition(Condition.EQUAL),
            new Criterion().setField("key2").setValue("value2").setCondition(Condition.EQUAL))));
    QueryBuilder queryBuilder = buildFilterQuery(filter);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/ComplexFilterQuery.json"));
  }

  @Test
  public void testBuildFilterQueryWithRangeFilter() throws IOException {
    Filter filter = new Filter().setCriteria(new CriterionArray(
        Arrays.asList(new Criterion().setField("key1").setValue("value1").setCondition(Condition.GREATER_THAN),
            new Criterion().setField("key1").setValue("value2").setCondition(Condition.LESS_THAN),
            new Criterion().setField("key2").setValue("value3").setCondition(Condition.GREATER_THAN_OR_EQUAL_TO),
            new Criterion().setField("key3").setValue("value4").setCondition(Condition.LESS_THAN_OR_EQUAL_TO))));
    QueryBuilder queryBuilder = buildFilterQuery(filter);
    assertEquals(queryBuilder.toString(), loadJsonFromResource("filterQuery/RangeFilterQuery.json"));
  }

  @Test
  public void testEscapeReservedCharacters() {
    assertEquals(escapeReservedCharacters("foobar"), "foobar");
    assertEquals(escapeReservedCharacters("**"), "\\*\\*");
    assertEquals(escapeReservedCharacters("()"), "\\(\\)");
    assertEquals(escapeReservedCharacters("{}"), "\\{\\}");
  }
}