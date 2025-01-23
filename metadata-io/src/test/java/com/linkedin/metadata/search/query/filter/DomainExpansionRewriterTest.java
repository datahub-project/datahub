package com.linkedin.metadata.search.query.filter;

import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.search.QueryFilterRewriterConfiguration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.elasticsearch.query.filter.DomainExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainExpansionRewriterTest
    extends BaseQueryFilterRewriterTest<DomainExpansionRewriter> {
  private static final String FIELD_NAME = "domains.keyword";
  private final String grandParentUrn = "urn:li:domain:grand";
  private final String parentUrn = "urn:li:domain:foo";
  private final String parentUrn2 = "urn:li:domain:foo2";
  private final String childUrn = "urn:li:domain:bar";
  private final String childUrn2 = "urn:li:domain:bar2";

  private OperationContext opContext;
  private GraphRetriever mockGraphRetriever;

  @BeforeMethod
  public void init() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    CachingAspectRetriever mockAspectRetriever = mock(CachingAspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);

    mockGraphRetriever = spy(GraphRetriever.class);
    RetrieverContext mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);

    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> entityRegistry,
            () ->
                io.datahubproject.metadata.context.RetrieverContext.builder()
                    .aspectRetriever(mockAspectRetriever)
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
                    .graphRetriever(mockGraphRetriever)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            null,
            null);
  }

  @Override
  OperationContext getOpContext() {
    return opContext;
  }

  @Override
  DomainExpansionRewriter getTestRewriter() {
    return DomainExpansionRewriter.builder()
        .config(QueryFilterRewriterConfiguration.ExpansionRewriterConfiguration.DEFAULT)
        .build();
  }

  @Override
  String getTargetField() {
    return FIELD_NAME;
  }

  @Override
  String getTargetFieldValue() {
    return parentUrn;
  }

  @Override
  Condition getTargetCondition() {
    return Condition.DESCENDANTS_INCL;
  }

  @Test
  public void testTermsQueryRewrite() {
    DomainExpansionRewriter test = getTestRewriter();

    TermsQueryBuilder notTheFieldQuery = QueryBuilders.termsQuery("notTheField", parentUrn);
    assertEquals(
        test.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .condition(Condition.DESCENDANTS_INCL)
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            notTheFieldQuery),
        notTheFieldQuery,
        "Expected no rewrite due to non-applicable field");

    TermsQueryBuilder disabledRewriteQuery = QueryBuilders.termsQuery(FIELD_NAME, parentUrn);
    assertEquals(
        test.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .condition(Condition.DESCENDANTS_INCL)
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .searchFlags(new SearchFlags().setRewriteQuery(false))
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            disabledRewriteQuery),
        disabledRewriteQuery,
        "Expected no rewrite due to disabled rewrite searchFlags");

    // Setup nested
    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(parentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf", childUrn, parentUrn, RelationshipDirection.INCOMING, null))));

    TermsQueryBuilder testQuery = QueryBuilders.termsQuery(FIELD_NAME, parentUrn);
    TermsQueryBuilder expectedRewrite = QueryBuilders.termsQuery(FIELD_NAME, childUrn, parentUrn);

    assertEquals(
        test.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .condition(Condition.DESCENDANTS_INCL)
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            testQuery),
        expectedRewrite,
        "Expected rewrite");
  }

  @Test
  public void testTermsQueryRewritePagination() {
    DomainExpansionRewriter test =
        DomainExpansionRewriter.builder()
            .config(
                new QueryFilterRewriterConfiguration.ExpansionRewriterConfiguration(true, 1, 100))
            .build();

    // Setup nested
    // Page 1
    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(grandParentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                1,
                "page2",
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        parentUrn,
                        grandParentUrn,
                        RelationshipDirection.INCOMING,
                        null))));

    // Page 2
    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(grandParentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            eq("page2"),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        parentUrn2,
                        grandParentUrn,
                        RelationshipDirection.INCOMING,
                        null))));

    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(parentUrn2, parentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                1,
                "page2",
                List.of(
                    new RelatedEntities(
                        "IsPartOf", childUrn, parentUrn, RelationshipDirection.INCOMING, null))));

    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(parentUrn2, parentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            eq("page2"),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf", childUrn2, parentUrn2, RelationshipDirection.INCOMING, null))));

    TermsQueryBuilder testQuery = QueryBuilders.termsQuery(FIELD_NAME, grandParentUrn);
    TermsQueryBuilder expectedRewrite =
        QueryBuilders.termsQuery(
            FIELD_NAME, childUrn, childUrn2, parentUrn, parentUrn2, grandParentUrn);

    assertEquals(
        test.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .condition(Condition.DESCENDANTS_INCL)
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            testQuery),
        expectedRewrite,
        "Expected rewrite");
  }

  @Test
  public void testNestedBoolQueryRewrite() {
    DomainExpansionRewriter test =
        DomainExpansionRewriter.builder()
            .config(
                new QueryFilterRewriterConfiguration.ExpansionRewriterConfiguration(true, 1, 100))
            .build();

    // Setup nested
    when(mockGraphRetriever.scrollRelatedEntities(
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(parentUrn)))),
            eq(List.of(DOMAIN_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(List.of("IsPartOf")),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf", childUrn, parentUrn, RelationshipDirection.INCOMING, null))));

    BoolQueryBuilder testQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
    testQuery.filter(
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(FIELD_NAME, parentUrn))));
    testQuery.filter(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("someField")));
    testQuery.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .should(
                QueryBuilders.boolQuery()
                    .minimumShouldMatch(1)
                    .should(QueryBuilders.termsQuery(FIELD_NAME, parentUrn))));
    testQuery.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .should(QueryBuilders.existsQuery("someField")));
    testQuery.must(
        QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(FIELD_NAME, parentUrn))));
    testQuery.must(QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("someField")));
    testQuery.mustNot(
        QueryBuilders.boolQuery()
            .mustNot(
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termsQuery(FIELD_NAME, parentUrn))));
    testQuery.mustNot(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("someField")));

    BoolQueryBuilder expectedRewrite = QueryBuilders.boolQuery().minimumShouldMatch(1);
    expectedRewrite.filter(
        QueryBuilders.boolQuery()
            .filter(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery(FIELD_NAME, childUrn, parentUrn))));
    expectedRewrite.filter(
        QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("someField")));
    expectedRewrite.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .should(
                QueryBuilders.boolQuery()
                    .minimumShouldMatch(1)
                    .should(QueryBuilders.termsQuery(FIELD_NAME, childUrn, parentUrn))));
    expectedRewrite.should(
        QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .should(QueryBuilders.existsQuery("someField")));
    expectedRewrite.must(
        QueryBuilders.boolQuery()
            .must(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termsQuery(FIELD_NAME, childUrn, parentUrn))));
    expectedRewrite.must(QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("someField")));
    expectedRewrite.mustNot(
        QueryBuilders.boolQuery()
            .mustNot(
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termsQuery(FIELD_NAME, childUrn, parentUrn))));
    expectedRewrite.mustNot(
        QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("someField")));

    assertEquals(
        test.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .condition(Condition.DESCENDANTS_INCL)
                .searchType(QueryFilterRewriterSearchType.FULLTEXT_SEARCH)
                .queryFilterRewriteChain(mock(QueryFilterRewriteChain.class))
                .build(false),
            testQuery),
        expectedRewrite,
        "Expected rewrite of nested and pass through of other fields.");
  }
}
