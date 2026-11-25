package com.linkedin.metadata.graph;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Answers;
import org.testng.annotations.Test;

/** The default {@link GraphService#getLineageCount} reads every page, not just the first. */
public class GraphServiceLineageCountTest {

  private static final int PAGE = 2;

  private static Urn dataset(String name) {
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)");
  }

  private static final Urn A = dataset("a");
  private static final Urn B = dataset("b");

  /**
   * A backend that pages like Neo4j: {@code total} is the full neighbour count, but each call
   * returns at most {@link #PAGE} relationships starting at {@code offset}.
   */
  private static GraphService pagingBackend(Urn source, List<Urn> neighbours) {
    GraphService service =
        mock(GraphService.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
    when(service.getLineage(
            any(OperationContext.class),
            eq(source),
            any(LineageGraphFilters.class),
            anyInt(),
            isNull(),
            eq(1)))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(3);
              List<LineageRelationship> page =
                  neighbours.stream()
                      .skip(offset)
                      .limit(PAGE)
                      .map(
                          urn ->
                              new LineageRelationship()
                                  .setEntity(urn)
                                  .setType("DownstreamOf")
                                  .setDegree(1))
                      .collect(Collectors.toList());
              return new EntityLineageResult()
                  .setStart(offset)
                  .setCount(page.size())
                  .setTotal(neighbours.size())
                  .setRelationships(new LineageRelationshipArray(page));
            });
    return service;
  }

  @Test
  public void testCountsNeighboursBeyondTheFirstPage() {
    List<Urn> neighbours =
        List.of(dataset("n1"), dataset("n2"), dataset("n3"), dataset("n4"), dataset("n5"));
    GraphService service = pagingBackend(A, neighbours);

    LineageCountResult result =
        service.getLineageCount(
            mock(OperationContext.class), Set.of(A), mock(LineageGraphFilters.class), 10);

    assertEquals(result.getTotal(), 5);
    assertEquals(new HashSet<>(result.getSample()), new HashSet<>(neighbours));
  }

  @Test
  public void testSharedNeighboursAndGroupMembersAcrossPages() {
    // A's neighbours span three pages and include B, a member of the counted group.
    List<Urn> fromA = List.of(dataset("n1"), dataset("n2"), B, dataset("shared"), dataset("n3"));
    GraphService service = pagingBackend(A, fromA);
    when(service.getLineage(
            any(OperationContext.class),
            eq(B),
            any(LineageGraphFilters.class),
            anyInt(),
            isNull(),
            eq(1)))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(3);
              List<LineageRelationship> page =
                  List.of(dataset("shared"), A).stream()
                      .skip(offset)
                      .limit(PAGE)
                      .map(
                          urn ->
                              new LineageRelationship()
                                  .setEntity(urn)
                                  .setType("DownstreamOf")
                                  .setDegree(1))
                      .collect(Collectors.toList());
              return new EntityLineageResult()
                  .setStart(offset)
                  .setCount(page.size())
                  .setTotal(2)
                  .setRelationships(new LineageRelationshipArray(page));
            });

    LineageCountResult result =
        service.getLineageCount(
            mock(OperationContext.class), Set.of(A, B), mock(LineageGraphFilters.class), 10);

    // n1, n2, n3 and shared, each once; A and B are the group itself.
    assertEquals(result.getTotal(), 4);
  }

  @Test
  public void testStopsOnAnEmptyPageWhenTotalOverstates() {
    GraphService service =
        mock(GraphService.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
    when(service.getLineage(
            any(OperationContext.class),
            eq(A),
            any(LineageGraphFilters.class),
            anyInt(),
            isNull(),
            eq(1)))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(3);
              List<LineageRelationship> page =
                  offset == 0
                      ? List.of(
                          new LineageRelationship()
                              .setEntity(dataset("n1"))
                              .setType("DownstreamOf")
                              .setDegree(1))
                      : List.of();
              // Claims 100 neighbours but only ever returns one: must not loop forever.
              return new EntityLineageResult()
                  .setStart(offset)
                  .setCount(page.size())
                  .setTotal(100)
                  .setRelationships(new LineageRelationshipArray(page));
            });

    assertEquals(
        service
            .getLineageCount(
                mock(OperationContext.class), Set.of(A), mock(LineageGraphFilters.class), 10)
            .getTotal(),
        1);
  }
}
