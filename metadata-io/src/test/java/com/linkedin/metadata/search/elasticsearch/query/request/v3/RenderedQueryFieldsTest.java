package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class RenderedQueryFieldsTest {

  @Test
  public void testRootsComeFromTheRenderedQuery() {
    QueryBuilder query =
        QueryBuilders.simpleQueryStringQuery("orders")
            .field("name.text")
            .field("description.stemmed^0.5")
            .field("other.text^0.2");
    assertEquals(
        Set.copyOf(RenderedQueryFields.roots(query)), Set.of("name", "description", "other"));
  }

  @Test
  public void testAMatchClauseContributesItsFieldRoot() {
    QueryBuilder query = QueryBuilders.matchQuery("name.text", "orders");
    assertEquals(RenderedQueryFields.roots(query), List.of("name"));
  }

  @Test
  public void testADirectOtherFieldIsARoot() {
    QueryBuilder query = QueryBuilders.matchQuery("other.keyword", "orders");
    assertEquals(RenderedQueryFields.roots(query), List.of("other"));
  }
}
