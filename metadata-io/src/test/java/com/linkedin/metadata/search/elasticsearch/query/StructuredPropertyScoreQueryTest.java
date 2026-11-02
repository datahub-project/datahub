package com.linkedin.metadata.search.elasticsearch.query;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public class StructuredPropertyScoreQueryTest {

  @Test
  public void testTheQuerySubtractsTheMinimum() {
    QueryBuilder query =
        ESSearchDAO.structuredPropertyScoreBrowseQuery(
                QueryBuilders.matchAllQuery(), "usageCount", 10, 20, 3)
            .orElseThrow();
    String rendered = query.toString();
    String compact = rendered.replace(" ", "").replace("\n", "");

    assertTrue(compact.contains("\"field\":\"usageCount\""), rendered);
    assertTrue(ESSearchDAO.FIELD_VALUE_MULTIPLIER_SCRIPT.contains("if (v <= min) { return 1.0; }"));
    assertTrue(
        ESSearchDAO.FIELD_VALUE_MULTIPLIER_SCRIPT.contains("return 1.0 + slope * (v - min);"));
    assertTrue(compact.contains("return1.0+slope*(v-min);"), rendered);
    assertTrue(compact.contains("\"min\":10.0"), rendered);
    assertTrue(compact.contains("\"max\":20.0"), rendered);
    assertTrue(compact.contains("\"cap\":3.0"), rendered);
    assertTrue(compact.contains("\"slope\":0.2"), rendered);
    assertTrue(
        ESSearchDAO.structuredPropertyScoreBrowseQuery(
                QueryBuilders.matchAllQuery(), "usageCount", -10, 0, 3)
            .isPresent());
    assertFalse(
        ESSearchDAO.structuredPropertyScoreBrowseQuery(
                QueryBuilders.matchAllQuery(), "usageCount", 20, 10, 3)
            .isPresent());
    assertFalse(
        ESSearchDAO.structuredPropertyScoreBrowseQuery(
                QueryBuilders.matchAllQuery(), "usageCount", 0, 10, 1)
            .isPresent());
  }
}
