package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import org.testng.annotations.Test;

public class SearchClauseBudgetTest {
  private static final SearchClauseBudget.FieldCounts DATASET_FIELDS =
      new SearchClauseBudget.FieldCounts(30, 0);

  @Test
  public void testTwentyTokensKeepEveryWordWithoutFuzziness() {
    String query = String.join(" ", Collections.nCopies(20, "zorbel"));
    assertEquals(SearchClauseBudget.firstTermsWithinBudget(query, DATASET_FIELDS), query);
    String escaped = V3SearchQueryBuilder.escapeSimpleQueryStringOperators(query);
    String sanitized = V3SearchQueryBuilder.splitAlphanumericTokens(escaped);
    assertEquals(SearchClauseBudget.fuzzyExpansions(sanitized, escaped, DATASET_FIELDS), 0);
  }

  @Test
  public void testTwoTokensKeepFullFuzziness() {
    String query = "customer orders";
    assertEquals(SearchClauseBudget.firstTermsWithinBudget(query, DATASET_FIELDS), query);
    String escaped = V3SearchQueryBuilder.escapeSimpleQueryStringOperators(query);
    String sanitized = V3SearchQueryBuilder.splitAlphanumericTokens(escaped);
    assertEquals(SearchClauseBudget.fuzzyExpansions(sanitized, escaped, DATASET_FIELDS), 10);
    assertEquals(SearchClauseBudget.prefixExpansions(escaped), 10);
  }

  @Test
  public void testZeroClauseRoomSkipsFuzzyExpansions() {
    SearchClauseBudget.FieldCounts packed = new SearchClauseBudget.FieldCounts(422, 0);
    assertEquals(SearchClauseBudget.fuzzyExpansions("zorbel", "zorbel", packed, "zorbel~2"), 0);
  }

  @Test
  public void testQuotedQueryDropsFuzziness() {
    String query = "\"customer orders\"";
    String escaped = V3SearchQueryBuilder.escapeSimpleQueryStringOperators(query);
    String sanitized = V3SearchQueryBuilder.splitAlphanumericTokens(escaped);
    assertEquals(SearchClauseBudget.fuzzyExpansions(sanitized, escaped, DATASET_FIELDS), 0);
  }
}
