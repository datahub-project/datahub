package com.linkedin.metadata.search.query.filter;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.IS_CHILD_OF_RELATIONSHIP_NAME;
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

import com.linkedin.metadata.aspect.AspectRetriever;
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
import com.linkedin.metadata.search.elasticsearch.query.filter.DocumentExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentExpansionRewriterTest
    extends BaseQueryFilterRewriterTest<DocumentExpansionRewriter> {
  private static final String FIELD_NAME = "parentDocument.keyword";
  private final String parentUrn = "urn:li:document:parent";
  private final String childUrn = "urn:li:document:child";

  private OperationContext opContext;
  private GraphRetriever mockGraphRetriever;

  @BeforeMethod
  public void init() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
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
  DocumentExpansionRewriter getTestRewriter() {
    return DocumentExpansionRewriter.builder()
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
    DocumentExpansionRewriter test = getTestRewriter();

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

    when(mockGraphRetriever.scrollRelatedEntities(
            eq(Set.of(DOCUMENT_ENTITY_NAME)),
            eq(
                QueryUtils.newDisjunctiveFilter(
                    buildCriterion("urn", Condition.EQUAL, List.of(parentUrn)))),
            eq(Set.of(DOCUMENT_ENTITY_NAME)),
            eq(EMPTY_FILTER),
            eq(Set.of(IS_CHILD_OF_RELATIONSHIP_NAME)),
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
                        IS_CHILD_OF_RELATIONSHIP_NAME,
                        childUrn,
                        parentUrn,
                        RelationshipDirection.INCOMING,
                        null))));

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
        "Expected rewrite to include nested child documents");
  }
}
