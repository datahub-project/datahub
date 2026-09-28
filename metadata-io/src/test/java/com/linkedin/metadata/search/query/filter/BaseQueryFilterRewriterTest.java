package com.linkedin.metadata.search.query.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.elasticsearch.query.filter.BaseQueryFilterRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.testng.annotations.Test;

public abstract class BaseQueryFilterRewriterTest<T extends BaseQueryFilterRewriter> {

  static final Set<Condition> HIERARCHY_CONDITIONS =
      Set.of(Condition.ANCESTORS_INCL, Condition.DESCENDANTS_INCL, Condition.RELATED_INCL);

  abstract OperationContext getOpContext();

  abstract T getTestRewriter();

  abstract String getTargetField();

  abstract String getTargetFieldValue();

  abstract Condition getTargetCondition();

  @Test
  public void testPreservedMinimumMatchRewrite() {
    BaseQueryFilterRewriter test = getTestRewriter();

    // Setup nested container
    BoolQueryBuilder testQuery = QueryBuilders.boolQuery().minimumShouldMatch(99);
    testQuery.filter(
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()))));
    testQuery.filter(QueryBuilders.existsQuery("someField"));
    testQuery.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(100)
            .should(
                QueryBuilders.boolQuery()
                    .minimumShouldMatch(101)
                    .should(QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()))));

    BoolQueryBuilder expectedRewrite = QueryBuilders.boolQuery().minimumShouldMatch(99);
    expectedRewrite.filter(
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()))));
    expectedRewrite.filter(QueryBuilders.existsQuery("someField"));
    expectedRewrite.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(100)
            .should(
                QueryBuilders.boolQuery()
                    .minimumShouldMatch(101)
                    .should(QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()))));

    assertEquals(
        test.rewrite(
            getOpContext(),
            QueryFilterRewriterContext.builder()
                .condition(getTargetCondition())
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            testQuery),
        expectedRewrite,
        "Expected preservation of minimumShouldMatch");
  }

  /**
   * A hierarchy rewriter must expand only for the three explicit hierarchical conditions and leave
   * every other condition alone. A catch-all {@code default:} in the condition switch instead gives
   * them all hierarchy semantics -- for container filters that means matching the parent's other
   * children, so a container's contents list its siblings and itself.
   *
   * <p>The sweep is over {@link Condition#values()} rather than a hand-picked pair so that a
   * condition added later has to be classified deliberately instead of inheriting whatever the
   * switch happens to do with it.
   */
  @Test
  public void testNonHierarchyConditionsNotExpanded() {
    BaseQueryFilterRewriter test = getTestRewriter();
    GraphRetriever graphRetriever = getOpContext().getRetrieverContext().getGraphRetriever();

    for (Condition condition : Condition.values()) {
      if (HIERARCHY_CONDITIONS.contains(condition)) {
        continue;
      }
      TermsQueryBuilder testQuery =
          QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue());

      assertEquals(
          test.rewrite(
              getOpContext(),
              QueryFilterRewriterContext.builder()
                  .condition(condition)
                  .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                  .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                  .build(false),
              testQuery),
          QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()),
          String.format("Expected no rewrite for condition %s", condition));
    }

    // An unstubbed retriever expands to nothing, so the equality assertion alone would pass even
    // when the rewriter traverses. Assert no traversal was attempted at all.
    verify(graphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * A rewriter declares the search types it applies to, and anything outside that set must be left
   * alone. Because {@code &&} binds tighter than {@code ||}, an unparenthesised search-type check
   * in the enablement gate is discarded whenever query rewriting is not explicitly disabled -- the
   * rewriter then runs on search types it never opted into, and throws outright when the request
   * carries no search flags at all.
   */
  @Test
  public void testUnsupportedSearchTypeNotRewritten() {
    BaseQueryFilterRewriter test = getTestRewriter();
    GraphRetriever graphRetriever = getOpContext().getRetrieverContext().getGraphRetriever();

    Set<QueryFilterRewriterSearchType> unsupportedSearchTypes =
        Arrays.stream(QueryFilterRewriterSearchType.values())
            .filter(searchType -> !test.getRewriterSearchTypes().contains(searchType))
            .collect(Collectors.toSet());
    assertFalse(
        unsupportedSearchTypes.isEmpty(),
        "Expected the rewriter to opt out of at least one search type, otherwise this proves nothing");

    // Both flag shapes matter: absent flags is the case that throws rather than over-expanding.
    for (SearchFlags searchFlags : Arrays.asList(new SearchFlags(), null)) {
      for (QueryFilterRewriterSearchType searchType : unsupportedSearchTypes) {
        TermsQueryBuilder testQuery =
            QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue());

        assertEquals(
            test.rewrite(
                getOpContext(),
                QueryFilterRewriterContext.builder()
                    .condition(getTargetCondition())
                    .searchType(searchType)
                    .searchFlags(searchFlags)
                    .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                    .build(false),
                testQuery),
            QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()),
            String.format("Expected no rewrite for unsupported search type %s", searchType));
      }
    }

    verify(graphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * {@code rewriteQuery=false} is the caller's kill switch, and it has to keep working for a search
   * type the rewriter does support.
   */
  @Test
  public void testRewriteQueryDisabledIsRespected() {
    BaseQueryFilterRewriter test = getTestRewriter();
    GraphRetriever graphRetriever = getOpContext().getRetrieverContext().getGraphRetriever();

    TermsQueryBuilder testQuery = QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue());

    assertEquals(
        test.rewrite(
            getOpContext(),
            QueryFilterRewriterContext.builder()
                .condition(getTargetCondition())
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .searchFlags(new SearchFlags().setRewriteQuery(false))
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            testQuery),
        QueryBuilders.termsQuery(getTargetField(), getTargetFieldValue()),
        "Expected no rewrite when rewriteQuery is disabled");

    verify(graphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }
}
