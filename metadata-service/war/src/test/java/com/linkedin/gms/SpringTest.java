package com.linkedin.gms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheScheduler;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.annotations.Test;

/** Production {@code application.yaml} with no overrides. */
public class SpringTest extends AbstractGmsSpringContextTest {

  private static final String PARENT_CONTAINER = "urn:li:container:parent";
  private static final String CHILD_CONTAINER = "urn:li:container:child";

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired private QueryFilterRewriteChain queryFilterRewriteChain;

  @Autowired
  @Qualifier("entityGraphCache")
  private EntityGraphCache entityGraphCache;

  @Test
  public void testTelemetry() {
    DailyReport dailyReport = this.applicationContext.getBean(DailyReport.class);
    assertNotNull(dailyReport);
  }

  @Test
  public void testRewriterChainLeavesEqualConditionUnexpanded() throws Exception {
    GraphRetriever graphRetriever = childOfParentGraph();
    OperationContext opContext = withGraphRetriever(graphRetriever);
    TermsQueryBuilder filter = QueryBuilders.termsQuery("container.keyword", PARENT_CONTAINER);

    TermsQueryBuilder rewritten = rewrite(opContext, Condition.EQUAL, filter);

    assertSame(rewritten, filter, "an EQUAL container filter must pass through unchanged");
    Mockito.verifyNoInteractions(graphRetriever);
  }

  @Test
  public void testRewriterChainExpandsDescendantsCondition() throws Exception {
    OperationContext opContext = withGraphRetriever(childOfParentGraph());
    TermsQueryBuilder filter = QueryBuilders.termsQuery("container.keyword", PARENT_CONTAINER);

    TermsQueryBuilder rewritten = rewrite(opContext, Condition.DESCENDANTS_INCL, filter);

    assertEquals(
        rewritten.values().stream().map(Object::toString).collect(Collectors.toSet()),
        Set.of(PARENT_CONTAINER, CHILD_CONTAINER));
  }

  @Test
  public void testEntityGraphCacheEnabledByDefault() {
    assertTrue(
        entityGraphCache instanceof EntityGraphCacheService,
        "datahub.gms.entityGraphCache.enabled defaults to true, so the real cache must be wired");
    assertTrue(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.CONTAINER).isPresent());
    assertNotNull(applicationContext.getBean(EntityGraphCacheScheduler.class));
  }

  private TermsQueryBuilder rewrite(
      OperationContext opContext, Condition condition, TermsQueryBuilder filter) {
    return QueryFilterRewriterContext.builder()
        .queryFilterRewriteChain(queryFilterRewriteChain)
        .searchType(QueryFilterRewriterSearchType.STRUCTURED_SEARCH)
        .condition(condition)
        .build(false)
        .rewrite(opContext, filter);
  }

  /** One {@code IsPartOf} edge: child container -> parent container. */
  private static GraphRetriever childOfParentGraph() {
    GraphRetriever graphRetriever = Mockito.mock(GraphRetriever.class);
    Mockito.doCallRealMethod()
        .when(graphRetriever)
        .consumeRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
    Mockito.when(
            graphRetriever.scrollRelatedEntities(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        // Always returning the same edge is enough: the second hop finds only the already-visited
        // child, so the walk stops.
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1,
                1,
                null,
                List.of(
                    new RelatedEntities(
                        "IsPartOf",
                        CHILD_CONTAINER,
                        PARENT_CONTAINER,
                        RelationshipDirection.INCOMING,
                        null))));
    return graphRetriever;
  }

  /**
   * The rebuilt retriever context keeps the default {@code EntityGraphCache.NO_OP}, so these
   * rewriter tests exercise the graph-walk path only, not the entity-graph-cache path.
   */
  private OperationContext withGraphRetriever(GraphRetriever graphRetriever) throws Exception {
    RetrieverContext current = systemOperationContext.getRetrieverContext();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .aspectRetriever(current.getAspectRetriever())
            .cachingAspectRetriever(current.getCachingAspectRetriever())
            .searchRetriever(current.getSearchRetriever())
            .build();
    return systemOperationContext.toBuilder()
        .retrieverContext(retrieverContext)
        .build(systemOperationContext.getSessionActorContext(), false);
  }
}
