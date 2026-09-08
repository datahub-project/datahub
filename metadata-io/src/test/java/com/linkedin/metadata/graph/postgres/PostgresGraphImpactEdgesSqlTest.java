package com.linkedin.metadata.graph.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.Test;

public class PostgresGraphImpactEdgesSqlTest {

  @Test
  public void datasetDownstreamOfOutgoingIncluded() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset")));
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("et.type_name = 'DownstreamOf'"));
    assertTrue(sql.contains("|| 'fwd'"));
    assertTrue(sql.contains("lower(split_part(vs.urn, ':', 3)) = 'dataset'"));
    assertFalse(sql.contains("|| 'rev'"));
  }

  @Test
  public void datasetDoesNotTraverseConsumesAsOutgoingCurrent() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset")));
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertFalse(
        sql.contains("et.type_name = 'Consumes'")
            && sql.contains("lower(split_part(vs.urn, ':', 3)) = 'dataset'"));
  }

  @Test
  public void incomingIsFlippedWithRevId() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "Consumes", RelationshipDirection.INCOMING, "dataJob")));
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("e.target_id AS source"));
    assertTrue(sql.contains("e.source_id AS target"));
    assertTrue(sql.contains("|| 'rev'"));
    assertFalse(sql.contains("|| 'fwd'"));
    assertTrue(sql.contains("lower(split_part(vt.urn, ':', 3)) = 'dataset'"));
    assertTrue(sql.contains("lower(split_part(vs.urn, ':', 3)) = 'datajob'"));
  }

  @Test
  public void undirectedExpandsBothOrientationsWithDistinctIds() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "RelatedTo", RelationshipDirection.UNDIRECTED, "dataset")));
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("|| 'fwd'"));
    assertTrue(sql.contains("|| 'rev'"));
  }

  @Test
  public void allowedRelationshipTypesHonored() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset"),
                new LineageRegistry.EdgeInfo(
                    "Consumes", RelationshipDirection.INCOMING, "dataJob")));
    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM, null, Set.of("DownstreamOf"), new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("et.type_name = 'DownstreamOf'"));
    assertFalse(sql.contains("et.type_name = 'Consumes'"));
  }

  @Test
  public void allowedEntityTypesHonored() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset"),
                new LineageRegistry.EdgeInfo(
                    "Consumes", RelationshipDirection.INCOMING, "dataJob")));
    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM, Set.of("dataset"), null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("et.type_name = 'DownstreamOf'"));
    assertFalse(sql.contains("et.type_name = 'Consumes'"));
  }

  @Test
  public void branchSelectIncludesViaAndLifecycleOwner() {
    LineageRegistry registry =
        registryWith(
            "dataset",
            List.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf", RelationshipDirection.OUTGOING, "dataset")));
    LineageGraphFilters filters =
        new LineageGraphFilters(LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
    String sql =
        PostgresGraphImpactEdgesSql.render(
            tables(), PostgresGraphImpactEdgesSql.enumerateBranches(registry, filters));
    assertTrue(sql.contains("e.properties->>'via'"));
    assertTrue(sql.contains("e.properties->>'lifecycleOwner'"));
    assertTrue(sql.contains("AS via"));
    assertTrue(sql.contains("AS lifecycle_owner"));
  }

  @Test
  public void sqlStringLiteralEscapesQuotes() {
    assertTrue(PostgresGraphImpactEdgesSql.sqlStringLiteral("a'b").equals("'a''b'"));
  }

  private static LineageRegistry registryWith(
      String entityType, List<LineageRegistry.EdgeInfo> downstream) {
    LineageRegistry registry = mock(LineageRegistry.class);
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    when(entityRegistry.getEntitySpec(anyString()))
        .thenAnswer(
            invocation -> {
              EntitySpec spec = mock(EntitySpec.class);
              when(spec.getName()).thenReturn(invocation.getArgument(0));
              return spec;
            });
    when(registry.getEntityRegistry()).thenReturn(entityRegistry);
    LineageRegistry.LineageSpec spec = new LineageRegistry.LineageSpec(List.of(), downstream);
    when(registry.getLineageSpecs()).thenReturn(Map.of(entityType, spec));
    when(registry.getLineageRelationships(entityType, LineageDirection.DOWNSTREAM))
        .thenReturn(downstream);
    when(registry.getLineageRelationships(entityType, LineageDirection.UPSTREAM))
        .thenReturn(List.of());
    return registry;
  }

  private static PostgresGraphTables tables() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.setSchema("public");
    props.getPgGraph().setEnabled(true);
    props.getPgGraph().setTablePrefix("metadata_graph");
    props.getPgGraph().setPartitionCount(2);
    props.getPgGraph().setIdHashAlgo("XXHASH64");
    props.getPgGraph().setMaxEdgeWriteBatchSize(1000);
    return new PostgresGraphTables(props);
  }
}
