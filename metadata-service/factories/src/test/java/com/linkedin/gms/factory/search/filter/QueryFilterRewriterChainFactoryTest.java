package com.linkedin.gms.factory.search.filter;

import static com.linkedin.metadata.Constants.IS_CHILD_OF_RELATIONSHIP_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link QueryFilterRewriterChainFactory} with the production {@code application.yaml} and
 * checks that each {@code searchService.queryFilterRewriter.*.enabled} switch decides whether the
 * chain search uses expands that filter field through the graph.
 */
public class QueryFilterRewriterChainFactoryTest {

  private static final String CONTAINER_FIELD = "container.keyword";
  private static final String DOMAIN_FIELD = "domains.keyword";
  private static final String DOCUMENT_FIELD = "parentDocument.keyword";

  private static final String CHILD_CONTAINER = "urn:li:container:child";
  private static final String PARENT_CONTAINER = "urn:li:container:parent";
  private static final String CHILD_DOMAIN = "urn:li:domain:child";
  private static final String PARENT_DOMAIN = "urn:li:domain:parent";
  private static final String CHILD_DOCUMENT = "urn:li:document:child";
  private static final String PARENT_DOCUMENT = "urn:li:document:parent";

  private final ApplicationContextRunner contextRunner =
      ProductionConfigContextRunner.create()
          .withUserConfiguration(QueryFilterRewriterChainFactory.class);

  @Test
  public void testProductionDefaultsExpandEveryHierarchyField() {
    contextRunner.run(
        context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBeansOfType(QueryFilterRewriter.class)).hasSize(3);
          assertExpanded(context, CONTAINER_FIELD, PARENT_CONTAINER, CHILD_CONTAINER);
          assertExpanded(context, DOMAIN_FIELD, PARENT_DOMAIN, CHILD_DOMAIN);
          assertExpanded(context, DOCUMENT_FIELD, PARENT_DOCUMENT, CHILD_DOCUMENT);
        });
  }

  @Test
  public void testEachSwitchDisablesOnlyItsOwnField() {
    contextRunner
        .withPropertyValues("searchService.queryFilterRewriter.containerExpansion.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertNotExpanded(context, CONTAINER_FIELD, PARENT_CONTAINER, CHILD_CONTAINER);
              assertExpanded(context, DOMAIN_FIELD, PARENT_DOMAIN, CHILD_DOMAIN);
              assertExpanded(context, DOCUMENT_FIELD, PARENT_DOCUMENT, CHILD_DOCUMENT);
            });
    contextRunner
        .withPropertyValues("searchService.queryFilterRewriter.domainExpansion.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertExpanded(context, CONTAINER_FIELD, PARENT_CONTAINER, CHILD_CONTAINER);
              assertNotExpanded(context, DOMAIN_FIELD, PARENT_DOMAIN, CHILD_DOMAIN);
              assertExpanded(context, DOCUMENT_FIELD, PARENT_DOCUMENT, CHILD_DOCUMENT);
            });
    contextRunner
        .withPropertyValues("searchService.queryFilterRewriter.documentExpansion.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertExpanded(context, CONTAINER_FIELD, PARENT_CONTAINER, CHILD_CONTAINER);
              assertExpanded(context, DOMAIN_FIELD, PARENT_DOMAIN, CHILD_DOMAIN);
              assertNotExpanded(context, DOCUMENT_FIELD, PARENT_DOCUMENT, CHILD_DOCUMENT);
            });
  }

  @Test
  public void testAllSwitchesOffLeavesAnEmptyChain() {
    contextRunner
        .withPropertyValues(
            "searchService.queryFilterRewriter.containerExpansion.enabled=false",
            "searchService.queryFilterRewriter.domainExpansion.enabled=false",
            "searchService.queryFilterRewriter.documentExpansion.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBeansOfType(QueryFilterRewriter.class)).isEmpty();
              assertNotExpanded(context, CONTAINER_FIELD, PARENT_CONTAINER, CHILD_CONTAINER);
              assertNotExpanded(context, DOMAIN_FIELD, PARENT_DOMAIN, CHILD_DOMAIN);
              assertNotExpanded(context, DOCUMENT_FIELD, PARENT_DOCUMENT, CHILD_DOCUMENT);
            });
  }

  @Test
  public void testExpansionPageSizeComesFromConfiguration() {
    contextRunner
        .withPropertyValues("searchService.queryFilterRewriter.containerExpansion.pageSize=7")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              OneHopGraphRetriever graph = new OneHopGraphRetriever();
              rewrite(context, graph, CONTAINER_FIELD, PARENT_CONTAINER);
              assertThat(graph.requestedPageSizes).isNotEmpty().containsOnly(7);
            });
  }

  /** A filter on {@code parent} also matches its descendants (the default condition). */
  private static void assertExpanded(
      ApplicationContext context, String field, String parent, String child) {
    TermsQueryBuilder rewritten = rewrite(context, new OneHopGraphRetriever(), field, parent);
    assertThat(rewritten.fieldName()).isEqualTo(field);
    assertThat(rewritten.values()).containsExactlyInAnyOrder(parent, child);
  }

  private static void assertNotExpanded(
      ApplicationContext context, String field, String parent, String child) {
    OneHopGraphRetriever graph = new OneHopGraphRetriever();
    TermsQueryBuilder rewritten = rewrite(context, graph, field, parent);
    assertThat(rewritten.values()).containsExactly(parent).doesNotContain(child);
    assertThat(graph.requestedPageSizes).isEmpty();
  }

  private static TermsQueryBuilder rewrite(
      ApplicationContext context, GraphRetriever graph, String field, String urn) {
    QueryFilterRewriteChain chain = context.getBean(QueryFilterRewriteChain.class);
    QueryFilterRewriterContext rewriterContext =
        QueryFilterRewriterContext.builder()
            .queryFilterRewriteChain(chain)
            .searchType(QueryFilterRewriterSearchType.STRUCTURED_SEARCH)
            .build(false);
    return rewriterContext.rewrite(opContext(graph), QueryBuilders.termsQuery(field, urn));
  }

  private static OperationContext opContext(GraphRetriever graph) {
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
            .graphRetriever(graph)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();
    return TestOperationContexts.systemContextNoSearchAuthorization(
        () -> entityRegistry, () -> retrieverContext, null);
  }

  /**
   * Each parent has exactly one child, linked by the relationship type its hierarchy uses. The
   * child is returned only for an INCOMING walk (child -> parent edges, read from the parent) over
   * that relationship type, so a rewriter wired with the wrong direction or relationship expands
   * nothing.
   */
  private static final class OneHopGraphRetriever implements GraphRetriever {
    private record Child(String urn, String relationshipType) {}

    private static final Map<String, Child> CHILD_OF =
        Map.of(
            PARENT_CONTAINER, new Child(CHILD_CONTAINER, "IsPartOf"),
            PARENT_DOMAIN, new Child(CHILD_DOMAIN, "IsPartOf"),
            PARENT_DOCUMENT, new Child(CHILD_DOCUMENT, IS_CHILD_OF_RELATIONSHIP_NAME));

    final List<Integer> requestedPageSizes = new ArrayList<>();

    @Nonnull
    @Override
    public RelatedEntitiesScrollResult scrollRelatedEntities(
        @Nullable Set<String> sourceTypes,
        @Nonnull Filter sourceEntityFilter,
        @Nullable Set<String> destinationTypes,
        @Nonnull Filter destinationEntityFilter,
        @Nonnull Set<String> relationshipTypes,
        @Nonnull RelationshipFilter relationshipFilter,
        @Nonnull List<SortCriterion> sortCriteria,
        @Nullable String scrollId,
        @Nullable Integer count,
        @Nullable Long startTimeMillis,
        @Nullable Long endTimeMillis) {
      requestedPageSizes.add(count);
      List<RelatedEntities> related = new ArrayList<>();
      if (relationshipFilter.getDirection() == RelationshipDirection.INCOMING) {
        sourceEntityFilter.getOr().stream()
            .flatMap(conjunction -> conjunction.getAnd().stream())
            .flatMap(criterion -> criterion.getValues().stream())
            .filter(CHILD_OF::containsKey)
            .map(CHILD_OF::get)
            .filter(child -> relationshipTypes.contains(child.relationshipType()))
            .forEach(
                child ->
                    related.add(
                        new RelatedEntities(
                            child.relationshipType(),
                            child.urn(),
                            child.urn(),
                            RelationshipDirection.INCOMING,
                            null)));
      }
      return new RelatedEntitiesScrollResult(related.size(), related.size(), null, related);
    }
  }
}
