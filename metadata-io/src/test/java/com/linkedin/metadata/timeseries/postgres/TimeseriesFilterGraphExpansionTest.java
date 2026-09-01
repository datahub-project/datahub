package com.linkedin.metadata.timeseries.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TimeseriesFilterGraphExpansionTest {

  private static final String SEED = "urn:li:container:root";
  private static final String CHILD = "urn:li:container:child";
  private static final String PARENT = "urn:li:container:parent";

  private OperationContext opContext;
  private GraphRetriever graphRetriever;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    graphRetriever = mock(GraphRetriever.class);
    when(opContext.getRetrieverContext()).thenReturn(retrieverContext);
    when(retrieverContext.getGraphRetriever()).thenReturn(graphRetriever);
  }

  @Test
  public void expand_emptySeeds_returnsEmpty() {
    Set<String> result =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, Condition.ANCESTORS_INCL, List.of());
    assertTrue(result.isEmpty());
  }

  @Test
  public void expand_nullGraphRetriever_returnsSeeds() {
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    when(opContext.getRetrieverContext()).thenReturn(retrieverContext);
    when(retrieverContext.getGraphRetriever()).thenReturn(null);

    Set<String> result =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, Condition.DESCENDANTS_INCL, List.of(SEED));
    assertEquals(result, Set.of(SEED));
  }

  @Test
  public void expand_ancestorsIncl_includesOutgoingRelated() {
    stubConsumeRelated(RelationshipDirection.OUTGOING, PARENT);

    Set<String> result =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, Condition.ANCESTORS_INCL, List.of(SEED));

    assertTrue(result.contains(SEED));
    assertTrue(result.contains(PARENT));
  }

  @Test
  public void expand_descendantsIncl_includesIncomingRelated() {
    stubConsumeRelated(RelationshipDirection.INCOMING, CHILD);

    Set<String> result =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, Condition.DESCENDANTS_INCL, List.of(SEED));

    assertTrue(result.contains(SEED));
    assertTrue(result.contains(CHILD));
  }

  @Test
  public void expand_relatedIncl_expandsOutgoingFromDescendants() {
    // Seed → child (INCOMING), then child → parent (OUTGOING from descendant set).
    // Starting OUTGOING only from seeds would miss PARENT when the edge is child→parent.
    doAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              Function<RelatedEntitiesScrollResult, Boolean> consumer = inv.getArgument(0);
              com.linkedin.metadata.query.filter.RelationshipFilter relFilter = inv.getArgument(6);
              if (relFilter.getDirection() == RelationshipDirection.INCOMING) {
                RelatedEntities related =
                    new RelatedEntities(
                        "IsPartOf", CHILD, SEED, RelationshipDirection.INCOMING, null);
                consumer.apply(
                    RelatedEntitiesScrollResult.builder()
                        .numResults(1)
                        .pageSize(100)
                        .entities(List.of(related))
                        .build());
              } else if (relFilter.getDirection() == RelationshipDirection.OUTGOING) {
                RelatedEntities related =
                    new RelatedEntities(
                        "IsPartOf", CHILD, PARENT, RelationshipDirection.OUTGOING, null);
                consumer.apply(
                    RelatedEntitiesScrollResult.builder()
                        .numResults(1)
                        .pageSize(100)
                        .entities(List.of(related))
                        .build());
              }
              return null;
            })
        .when(graphRetriever)
        .consumeRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), isNull(), isNull());

    Set<String> result =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, Condition.RELATED_INCL, List.of(SEED));

    assertTrue(result.contains(SEED));
    assertTrue(result.contains(CHILD));
    assertTrue(result.contains(PARENT));
  }

  @Test
  public void expand_unsupportedCondition_throws() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            TimeseriesFilterGraphExpansion.expandForLineageCondition(
                opContext, Condition.EQUAL, List.of(SEED)));
  }

  @Test
  public void expandUrns_multipleSeeds_doesNotStopBeforeRelatedPages() {
    String extra1 = "urn:li:container:extra1";
    String extra2 = "urn:li:container:extra2";
    doAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              Function<RelatedEntitiesScrollResult, Boolean> consumer = inv.getArgument(0);
              RelatedEntities first =
                  new RelatedEntities(
                      "IsPartOf", SEED, extra1, RelationshipDirection.OUTGOING, null);
              Boolean stop =
                  consumer.apply(
                      RelatedEntitiesScrollResult.builder()
                          .numResults(1)
                          .pageSize(1)
                          .entities(List.of(first))
                          .build());
              if (Boolean.TRUE.equals(stop)) {
                return null;
              }
              RelatedEntities second =
                  new RelatedEntities(
                      "IsPartOf", SEED, extra2, RelationshipDirection.OUTGOING, null);
              consumer.apply(
                  RelatedEntitiesScrollResult.builder()
                      .numResults(1)
                      .pageSize(1)
                      .entities(List.of(second))
                      .build());
              return null;
            })
        .when(graphRetriever)
        .consumeRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), isNull(), isNull());

    Set<Urn> seeds =
        new HashSet<>(
            List.of(UrnUtils.getUrn(SEED), UrnUtils.getUrn(CHILD), UrnUtils.getUrn(PARENT)));
    Set<Urn> expanded =
        TimeseriesFilterGraphExpansion.expandUrns(
            graphRetriever, seeds, Set.of("IsPartOf"), RelationshipDirection.OUTGOING, 1, 5);

    assertTrue(expanded.contains(UrnUtils.getUrn(extra2)));
  }

  private void stubConsumeRelated(RelationshipDirection direction, String relatedUrn) {
    doAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              Function<RelatedEntitiesScrollResult, Boolean> consumer = inv.getArgument(0);
              RelatedEntities related =
                  direction == RelationshipDirection.OUTGOING
                      ? new RelatedEntities("IsPartOf", SEED, relatedUrn, direction, null)
                      : new RelatedEntities("IsPartOf", relatedUrn, SEED, direction, null);
              consumer.apply(
                  RelatedEntitiesScrollResult.builder()
                      .numResults(1)
                      .pageSize(100)
                      .entities(List.of(related))
                      .build());
              return null;
            })
        .when(graphRetriever)
        .consumeRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), isNull(), isNull());
  }
}
