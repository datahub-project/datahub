package com.linkedin.metadata.graph;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class LineageGraphFiltersTest {

  private OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testEmptyFilters() {
    // Check that EMPTY instance is initialized correctly
    assertEquals(LineageGraphFilters.EMPTY.getLineageDirection(), LineageDirection.$UNKNOWN);
    assertEquals(LineageGraphFilters.EMPTY.getAllowedEntityTypes(), Set.of());
    assertEquals(LineageGraphFilters.EMPTY.getAllowedRelationshipTypes(), Set.of());
    assertTrue(LineageGraphFilters.EMPTY.getEdgesPerEntityType().isEmpty());
  }

  @Test
  public void testForEntityType() {
    LineageRegistry mockLineageRegistry = spy(opContext.getLineageRegistry());

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            mockLineageRegistry, "dataset", LineageDirection.DOWNSTREAM);

    assertEquals(filters.getLineageDirection(), LineageDirection.DOWNSTREAM);
    assertEquals(
        filters.getAllowedEntityTypes(),
        Set.of(
            "mlModelGroup",
            "dataProcess",
            "dataJob",
            "mlModel",
            "mlFeature",
            "dataProcessInstance",
            "dataset",
            "chart",
            "dashboard",
            "mlPrimaryKey"));
    assertNull(filters.getAllowedRelationshipTypes());
    assertTrue(filters.getEdgesPerEntityType().isEmpty());

    verify(mockLineageRegistry).getEntitiesWithLineageToEntityType("dataset");
  }

  @Test
  public void testWithEntityTypes() {
    Set<String> allowedEntityTypes = Set.of("dataset", "chart");

    LineageGraphFilters filters =
        LineageGraphFilters.withEntityTypes(LineageDirection.UPSTREAM, allowedEntityTypes);

    assertEquals(filters.getLineageDirection(), LineageDirection.UPSTREAM);
    assertEquals(filters.getAllowedEntityTypes(), allowedEntityTypes);
    assertNull(filters.getAllowedRelationshipTypes());
    assertTrue(filters.getEdgesPerEntityType().isEmpty());
  }

  @Test
  public void testGetEdgeInfo_WithNoFilters() {
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());

    LineageRegistry mockLineageRegistry = spy(opContext.getLineageRegistry());
    Set<LineageRegistry.EdgeInfo> result = filters.getEdgeInfo(mockLineageRegistry, "dataset");

    assertTrue(result.size() >= 8);
    assertEquals(filters.getEdgesPerEntityType().size(), 1);
    assertTrue(filters.getEdgesPerEntityType().containsKey("dataset"));

    // Call again to verify computation happens only once
    Set<LineageRegistry.EdgeInfo> secondResult =
        filters.getEdgeInfo(mockLineageRegistry, "dataset");
    assertEquals(secondResult, result);

    verify(mockLineageRegistry, times(1))
        .getLineageRelationships("dataset", LineageDirection.DOWNSTREAM);
  }

  @Test
  public void testGetEdgeInfo_WithEntityTypeFilter() {
    LineageRegistry mockLineageRegistry = spy(opContext.getLineageRegistry());

    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM, Set.of("chart"), null, new ConcurrentHashMap<>());

    Set<LineageRegistry.EdgeInfo> result = filters.getEdgeInfo(mockLineageRegistry, "dataset");

    assertEquals(result.size(), 1);
    assertTrue(
        result.contains(
            new LineageRegistry.EdgeInfo("Consumes", RelationshipDirection.INCOMING, "chart")));
  }

  @Test
  public void testGetEdgeInfo_WithRelationshipTypeFilter() {
    LineageRegistry mockLineageRegistry = spy(opContext.getLineageRegistry());

    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM, null, Set.of("DownstreamOf"), new ConcurrentHashMap<>());

    Set<LineageRegistry.EdgeInfo> result = filters.getEdgeInfo(mockLineageRegistry, "dataset");

    assertTrue(result.size() >= 1);
    assertTrue(
        result.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.INCOMING, "dataset")));
  }

  @Test
  public void testGetEdgeInfo_WithEntityNameFix() {
    LineageRegistry mockLineageRegistry = spy(opContext.getLineageRegistry());

    // Case where entity type name in EdgeInfo doesn't match the spec name
    when(mockLineageRegistry.getLineageRelationships("dataset", LineageDirection.DOWNSTREAM))
        .thenReturn(
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.INCOMING, "DATASET")));

    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM, null, Set.of("DownstreamOf"), new ConcurrentHashMap<>());

    Set<LineageRegistry.EdgeInfo> result = filters.getEdgeInfo(mockLineageRegistry, "dataset");

    verify(mockLineageRegistry, times(2))
        .getLineageRelationships("dataset", LineageDirection.DOWNSTREAM);

    assertTrue(result.size() >= 1);
    assertTrue(
        result.contains(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.INCOMING, "dataset")));
  }

  @Test
  public void testStreamEdgeInfo() {
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            opContext.getLineageRegistry(), "chart", LineageDirection.UPSTREAM);
    // populate steam
    filters.getEdgeInfo(opContext.getLineageRegistry(), "chart");

    List<Pair<String, LineageRegistry.EdgeInfo>> streamResult = filters.streamEdgeInfo().toList();

    assertEquals(streamResult.size(), 1);
    assertTrue(
        streamResult.contains(
            Pair.of(
                "chart",
                new LineageRegistry.EdgeInfo(
                    "Consumes", RelationshipDirection.OUTGOING, "dataset"))));

    assertTrue(filters.containsEdgeInfo("chart", streamResult.get(0).getValue()));
    assertFalse(
        filters.containsEdgeInfo(
            "chart",
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.INCOMING, "dataset")));
  }
}
