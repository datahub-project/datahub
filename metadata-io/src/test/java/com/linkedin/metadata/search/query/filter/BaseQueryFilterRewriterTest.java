package com.linkedin.metadata.search.query.filter;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.elasticsearch.query.filter.BaseQueryFilterRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import io.datahubproject.metadata.context.OperationContext;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

public abstract class BaseQueryFilterRewriterTest<T extends BaseQueryFilterRewriter> {

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
}
