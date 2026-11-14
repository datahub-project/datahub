package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class SearchTemplateFieldPrunerTest {

  @Test
  public void testEmptyFieldListsAndBlankKeysAreDropped() throws Exception {
    String rendered =
        """
        {"dis_max": {"queries": [
          {"simple_query_string": {"query": "orders", "fields": []}},
          {"term": {"": {"value": "orders"}}},
          {"exists": {"field": ""}},
          {"multi_match": {"query": "orders", "fields": ["", "name.text^2"]}}
        ]}}
        """;

    JsonNode pruned = read(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of()));

    JsonNode queries = pruned.path("dis_max").path("queries");
    assertEquals(queries.size(), 1, pruned.toString());
    assertEquals(queries.get(0).path("multi_match").path("fields").toString(), "[\"name.text^2\"]");
  }

  @Test
  public void testRequiredShouldThatEmptiesPrunesTheBool() {
    String rendered =
        """
        {"bool": {
          "filter": [{"term": {"platform": "hive"}}],
          "should": [{"simple_query_string": {"query": "orders", "fields": []}}],
          "minimum_should_match": 1
        }}
        """;

    assertNull(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of()));
  }

  @Test
  public void testOptionalShouldThatEmptiesKeepsTheBool() throws Exception {
    String rendered =
        """
        {"bool": {
          "filter": [{"term": {"platform": "hive"}}],
          "should": [{"simple_query_string": {"query": "orders", "fields": []}}]
        }}
        """;

    JsonNode pruned = read(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of()));

    assertEquals(pruned.path("bool").path("filter").size(), 1);
    assertEquals(pruned.path("bool").has("should"), false);
  }

  @Test
  public void testFullyPrunedBodyIsNull() {
    String rendered =
        """
        {"dis_max": {"queries": [{"simple_query_string": {"query": "orders", "fields": []}}]}}
        """;

    assertNull(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of()));
  }

  @Test
  public void testRemovedFieldIsDropped() throws Exception {
    String rendered =
        """
        {"multi_match": {"query": "orders", "fields": ["description.text", "name.text"]}}
        """;

    JsonNode pruned =
        read(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of("description")));

    assertEquals(pruned.path("multi_match").path("fields").toString(), "[\"name.text\"]");
  }

  @Test
  public void testObjectBoolClausesAreKept() throws Exception {
    String rendered =
        """
        {"bool": {
          "must": {"match": {"name": "orders"}},
          "filter": {"term": {"platform": "hive"}}
        }}
        """;

    JsonNode pruned = read(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of()));

    assertEquals(pruned.path("bool").path("must").size(), 1, pruned.toString());
    assertEquals(
        pruned.path("bool").path("must").get(0).path("match").path("name").asText(), "orders");
    assertEquals(pruned.path("bool").path("filter").size(), 1, pruned.toString());
    assertEquals(
        pruned.path("bool").path("filter").get(0).path("term").path("platform").asText(), "hive");
  }

  @Test
  public void testObjectClauseOnRemovedFieldIsDropped() throws Exception {
    String rendered =
        """
        {"bool": {
          "filter": [{"term": {"platform": "hive"}}],
          "must": {"match": {"description": "orders"}}
        }}
        """;

    JsonNode pruned =
        read(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of("description")));

    assertEquals(pruned.path("bool").path("filter").size(), 1, pruned.toString());
    assertEquals(pruned.path("bool").has("must"), false, pruned.toString());
  }

  @Test
  public void testObjectClauseThatEmptiesTheBoolIsNull() {
    String rendered =
        """
        {"bool": {"must": {"match": {"description": "orders"}}}}
        """;

    assertNull(SearchTemplateFieldPruner.pruneRendered(rendered, Set.of("description")));
  }

  @Test
  public void testClausesBodyWithObjectFilterIsKept() throws Exception {
    String clauses =
        """
        {"filter": {"term": {"platform": "hive"}}}
        """;

    JsonNode pruned = read(SearchTemplateFieldPruner.pruneRenderedClauses(clauses, Set.of()));

    assertEquals(pruned.path("filter").size(), 1, pruned.toString());
    assertEquals(pruned.path("filter").get(0).path("term").path("platform").asText(), "hive");
  }

  @Test
  public void testClausesWithNoRemainingListAreNull() {
    String clauses =
        """
        {"should": [{"term": {"": {"value": "orders"}}}]}
        """;

    assertNull(SearchTemplateFieldPruner.pruneRenderedClauses(clauses, Set.of()));
  }

  @Test
  public void testPruneOfEmptiedQueryIsMatchNone() {
    QueryBuilder built =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("description", "orders"));

    QueryBuilder pruned = SearchTemplateFieldPruner.prune(built, Set.of("description"));

    assertTrue(pruned instanceof MatchNoneQueryBuilder, pruned.toString());
  }

  @Test
  public void testPruneLeavesMatchNoneAlone() {
    QueryBuilder pruned =
        SearchTemplateFieldPruner.prune(new MatchNoneQueryBuilder(), Set.of("description"));

    assertTrue(pruned instanceof MatchNoneQueryBuilder, pruned.toString());
  }

  private static JsonNode read(String json) throws Exception {
    return SearchTemplateMappers.JSON.readTree(json);
  }
}
