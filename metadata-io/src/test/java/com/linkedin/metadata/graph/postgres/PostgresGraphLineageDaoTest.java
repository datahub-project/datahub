package com.linkedin.metadata.graph.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.Test;

public class PostgresGraphLineageDaoTest {

  private static final OperationContext OP =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void queryViaInsertedBetweenParentAndChild() throws Exception {
    Urn upstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,up,PROD)");
    Urn query = urn("urn:li:query:q1");
    Urn downstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,down,PROD)");
    Map<Urn, PostgresGraphLineageDao.ParentHop> parents = new HashMap<>();
    parents.put(
        downstream, new PostgresGraphLineageDao.ParentHop(upstream, query.toString(), null));
    UrnArray path = PostgresGraphLineageDao.buildPathFromHops(upstream, downstream, parents);
    assertEquals(path, new UrnArray(List.of(upstream, query, downstream)));
  }

  @Test
  public void jobNodePathDoesNotInventVia() throws Exception {
    Urn upstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,up,PROD)");
    Urn job = urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),job)");
    Urn downstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,down,PROD)");
    Map<Urn, PostgresGraphLineageDao.ParentHop> parents = new HashMap<>();
    parents.put(job, new PostgresGraphLineageDao.ParentHop(upstream, null, null));
    parents.put(downstream, new PostgresGraphLineageDao.ParentHop(job, null, null));
    assertEquals(
        PostgresGraphLineageDao.buildPathFromHops(upstream, job, parents),
        new UrnArray(List.of(upstream, job)));
    assertEquals(
        PostgresGraphLineageDao.buildPathFromHops(upstream, downstream, parents),
        new UrnArray(List.of(upstream, job, downstream)));
  }

  @Test
  public void impactQueryViaPathAndViaRow() throws Exception {
    Urn upstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,up,PROD)");
    Urn query = urn("urn:li:query:q1");
    Urn downstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,down,PROD)");
    long upId = UrnFingerprint64.ofUtf8String(upstream.toString());
    long downId = UrnFingerprint64.ofUtf8String(downstream.toString());
    PostgresGraphPgRoutingDao pgRouting = mock(PostgresGraphPgRoutingDao.class);
    when(pgRouting.breadthFirstSearch(any(), any(), anyInt()))
        .thenReturn(
            List.of(
                new PostgresGraphPgRoutingDao.ReachedNode(
                    downstream, 1, "DownstreamOf", downId, upId, query.toString(), null)));
    PostgresGraphLineageDao dao = dao(pgRouting, mockRegistry());
    EntityLineageResult result = dao.getImpactLineage(OP, upstream, downstreamFilters(), 1);
    LineageRelationship dest = byEntity(result, downstream);
    assertNotNull(dest);
    assertEquals(dest.getPaths().get(0), new UrnArray(List.of(upstream, query, downstream)));
    LineageRelationship via = byEntity(result, query);
    assertNotNull(via);
    assertEquals(via.getPaths().get(0), new UrnArray(List.of(upstream, query)));
  }

  @Test
  public void impactJobNodePathsHaveNoViaRow() throws Exception {
    Urn upstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,up,PROD)");
    Urn job = urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),job)");
    Urn downstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,down,PROD)");
    long upId = UrnFingerprint64.ofUtf8String(upstream.toString());
    long jobId = UrnFingerprint64.ofUtf8String(job.toString());
    long downId = UrnFingerprint64.ofUtf8String(downstream.toString());
    PostgresGraphPgRoutingDao pgRouting = mock(PostgresGraphPgRoutingDao.class);
    when(pgRouting.breadthFirstSearch(any(), any(), anyInt()))
        .thenReturn(
            List.of(
                new PostgresGraphPgRoutingDao.ReachedNode(
                    job, 1, "Consumes", jobId, upId, null, null),
                new PostgresGraphPgRoutingDao.ReachedNode(
                    downstream, 2, "Produces", downId, jobId, null, null)));
    PostgresGraphLineageDao dao = dao(pgRouting, mockRegistry());
    EntityLineageResult result = dao.getImpactLineage(OP, upstream, downstreamFilters(), 2);
    assertEquals(byEntity(result, job).getPaths().get(0), new UrnArray(List.of(upstream, job)));
    assertEquals(
        byEntity(result, downstream).getPaths().get(0),
        new UrnArray(List.of(upstream, job, downstream)));
    assertEquals(result.getRelationships().size(), 2);
  }

  @Test
  public void impactFirstDestinationWinsWithLifecycleOnPath() throws Exception {
    Urn upstream = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,up,PROD)");
    Urn dest = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,down,PROD)");
    Urn owner1 = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,owner1,PROD)");
    Urn owner2 = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,owner2,PROD)");
    long upId = UrnFingerprint64.ofUtf8String(upstream.toString());
    long destId = UrnFingerprint64.ofUtf8String(dest.toString());
    PostgresGraphPgRoutingDao pgRouting = mock(PostgresGraphPgRoutingDao.class);
    when(pgRouting.breadthFirstSearch(any(), any(), anyInt()))
        .thenReturn(
            List.of(
                new PostgresGraphPgRoutingDao.ReachedNode(
                    dest, 1, "DownstreamOf", destId, upId, null, owner1.toString()),
                new PostgresGraphPgRoutingDao.ReachedNode(
                    dest, 1, "DownstreamOf", destId, upId, null, owner2.toString())));
    PostgresGraphLineageDao dao = dao(pgRouting, mockRegistry());
    EntityLineageResult result = dao.getImpactLineage(OP, upstream, downstreamFilters(), 1);
    assertEquals(result.getRelationships().size(), 1);
    assertEquals(
        byEntity(result, dest).getPaths().get(0), new UrnArray(List.of(upstream, owner1, dest)));
    assertNull(byEntity(result, owner1));
  }

  @Test
  public void getLineageCarriesViaOnLaterHops() throws Exception {
    Urn d1 = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,d1,PROD)");
    Urn d2 = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,d2,PROD)");
    Urn d3 = urn("urn:li:dataset:(urn:li:dataPlatform:hdfs,d3,PROD)");
    Urn q1 = urn("urn:li:query:q1");
    Urn q2 = urn("urn:li:query:q2");
    PostgresGraphOneHopDao oneHop = mock(PostgresGraphOneHopDao.class);
    when(oneHop.findRelatedForLineage(any(), any(), anyInt(), isNull()))
        .thenAnswer(
            invocation -> {
              GraphFilters gf = invocation.getArgument(1);
              String source = sourceUrn(gf);
              if (d1.toString().equals(source)) {
                return List.of(
                    new PostgresGraphOneHopDao.LineageHopRow(
                        new RelatedEntity("DownstreamOf", d2.toString(), q1.toString()), null));
              }
              if (d2.toString().equals(source)) {
                return List.of(
                    new PostgresGraphOneHopDao.LineageHopRow(
                        new RelatedEntity("DownstreamOf", d3.toString(), q2.toString()), null));
              }
              return List.of();
            });
    PostgresGraphLineageDao dao =
        new PostgresGraphLineageDao(
            oneHop, mock(PostgresGraphPgRoutingDao.class), mockRegistry(), graphConfig());
    EntityLineageResult result = dao.getLineage(OP, d1, downstreamFilters(), 0, 100, 2);
    LineageRelationship hop2 = byEntity(result, d3);
    assertNotNull(hop2);
    assertEquals(hop2.getPaths().get(0), new UrnArray(List.of(d1, q1, d2, q2, d3)));
    assertEquals(byEntity(result, q1).getPaths().get(0), new UrnArray(List.of(d1, q1)));
    assertEquals(byEntity(result, q2).getPaths().get(0), new UrnArray(List.of(d1, q1, d2, q2)));
  }

  private static String sourceUrn(GraphFilters gf) {
    return gf.getSourceEntityFilter().getOr().get(0).getAnd().get(0).getValues().get(0);
  }

  private static LineageRelationship byEntity(EntityLineageResult result, Urn urn) {
    for (LineageRelationship rel : result.getRelationships()) {
      if (urn.equals(rel.getEntity())) {
        return rel;
      }
    }
    return null;
  }

  private static PostgresGraphLineageDao dao(
      PostgresGraphPgRoutingDao pgRouting, LineageRegistry registry) {
    return new PostgresGraphLineageDao(
        mock(PostgresGraphOneHopDao.class), pgRouting, registry, graphConfig());
  }

  private static GraphServiceConfiguration graphConfig() {
    return TEST_GRAPH_SERVICE_CONFIG.toBuilder().type("postgres").build();
  }

  private static LineageGraphFilters downstreamFilters() {
    return new LineageGraphFilters(
        LineageDirection.DOWNSTREAM, null, null, new ConcurrentHashMap<>());
  }

  private static LineageRegistry mockRegistry() {
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
    List<LineageRegistry.EdgeInfo> downstream =
        List.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, "dataset"));
    when(registry.getLineageSpecs())
        .thenReturn(Map.of("dataset", new LineageRegistry.LineageSpec(List.of(), downstream)));
    when(registry.getLineageRelationships(anyString(), any()))
        .thenAnswer(
            invocation -> {
              String entityType = invocation.getArgument(0);
              LineageDirection direction = invocation.getArgument(1);
              if ("dataset".equals(entityType) && direction == LineageDirection.DOWNSTREAM) {
                return downstream;
              }
              return List.of();
            });
    return registry;
  }

  private static Urn urn(String value) throws Exception {
    Urn parsed = Urn.createFromString(value);
    assertNotNull(parsed);
    return parsed;
  }
}
