package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

/** A must or filter from template clauses narrows the body; it never makes the body optional. */
public class SearchTemplateMergeClausesTest {
  private static final String SCORE_ONLY =
      "{ \"must\": [ { \"bool\": { \"should\": [ { \"match_all\": { \"boost\": 0.0 } } ] } } ] }";

  @Test
  public void shouldOnlyBodyStillNeedsOneOfItsClauses() {
    BoolQueryBuilder body =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "orders"))
            .should(QueryBuilders.termQuery("id", "orders"));
    BoolQueryBuilder merged = (BoolQueryBuilder) SearchTemplateJson.mergeClauses(body, SCORE_ONLY);
    assertEquals(merged.minimumShouldMatch(), "1");
    assertEquals(merged.should().size(), 2);
    assertEquals(merged.must().size(), 1);
  }

  @Test
  public void filterClausesAndNegativeBodiesKeepTheShouldRequirement() {
    BoolQueryBuilder negative =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "orders"))
            .mustNot(QueryBuilders.termQuery("removed", true));
    BoolQueryBuilder filtered =
        (BoolQueryBuilder)
            SearchTemplateJson.mergeClauses(
                negative, "{ \"filter\": [ { \"term\": { \"platform\": \"x\" } } ] }");
    assertEquals(filtered.minimumShouldMatch(), "1");
    assertEquals(filtered.filter().size(), 1);
    assertEquals(filtered.mustNot().size(), 1);

    BoolQueryBuilder body =
        QueryBuilders.boolQuery().should(QueryBuilders.termQuery("name", "orders"));
    BoolQueryBuilder boosted =
        (BoolQueryBuilder)
            SearchTemplateJson.mergeClauses(
                body, "{ \"should\": [ { \"term\": { \"id\": \"orders\" } } ] }");
    // A should from the clauses counts toward the same minimum.
    assertEquals(boosted.minimumShouldMatch(), "1");
    assertEquals(boosted.should().size(), 2);
  }

  @Test
  public void otherBodiesKeepTheirMatching() {
    QueryBuilder disMax = QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("name", "x"));
    BoolQueryBuilder wrapped =
        (BoolQueryBuilder) SearchTemplateJson.mergeClauses(disMax, SCORE_ONLY);
    assertSame(wrapped.must().get(0), disMax);
    assertNull(wrapped.minimumShouldMatch());

    BoolQueryBuilder required =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("name", "x"))
            .should(QueryBuilders.termQuery("id", "x"));
    assertNull(
        ((BoolQueryBuilder) SearchTemplateJson.mergeClauses(required, SCORE_ONLY))
            .minimumShouldMatch());

    BoolQueryBuilder explicit =
        QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "x"))
            .should(QueryBuilders.termQuery("id", "x"))
            .minimumShouldMatch(2);
    assertEquals(
        ((BoolQueryBuilder) SearchTemplateJson.mergeClauses(explicit, SCORE_ONLY))
            .minimumShouldMatch(),
        "2");
  }
}
