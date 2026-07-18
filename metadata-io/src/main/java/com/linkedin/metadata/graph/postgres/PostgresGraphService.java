package com.linkedin.metadata.graph.postgres;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL-backed {@link GraphService} using SqlSetup pgRouting tables for reads and delegating
 * mutations to {@link PostgresGraphWriteSink}.
 */
@Slf4j
public class PostgresGraphService implements GraphService {

  @Getter private final GraphServiceConfiguration graphServiceConfig;
  @Getter private final LineageRegistry lineageRegistry;

  private final PostgresGraphWriteSink writeSink;
  private final PostgresGraphOneHopDao oneHopDao;
  private final PostgresGraphLineageDao lineageDao;
  private boolean writable = true;

  public PostgresGraphService(
      @Nonnull GraphServiceConfiguration graphServiceConfig,
      @Nonnull LineageRegistry lineageRegistry,
      @Nonnull PostgresGraphWriteSink writeSink,
      @Nonnull PostgresGraphOneHopDao oneHopDao,
      @Nonnull PostgresGraphLineageDao lineageDao) {
    this.graphServiceConfig = graphServiceConfig;
    this.lineageRegistry = lineageRegistry;
    this.writeSink = writeSink;
    this.oneHopDao = oneHopDao;
    this.lineageDao = lineageDao;
  }

  public void setWritable(boolean writable) {
    this.writable = writable;
  }

  @Override
  public void addEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    if (!writable) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    writeSink.addEdge(edge);
  }

  @Override
  public void upsertEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    addEdge(opContext, edge);
  }

  @Override
  public void removeEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    if (!writable) {
      return;
    }
    writeSink.removeEdge(edge);
  }

  @Override
  public void addEdges(@Nonnull OperationContext opContext, @Nonnull List<Edge> edges) {
    if (!writable) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    if (edges.isEmpty()) {
      return;
    }
    writeSink.addEdges(edges);
  }

  @Override
  public void upsertEdges(@Nonnull OperationContext opContext, @Nonnull List<Edge> edges) {
    addEdges(opContext, edges);
  }

  @Override
  public void removeEdges(@Nonnull OperationContext opContext, @Nonnull List<Edge> edges) {
    if (!writable) {
      return;
    }
    if (edges.isEmpty()) {
      return;
    }
    writeSink.removeEdges(edges);
  }

  @Nonnull
  @Override
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      final int offset,
      @Nullable final Integer count) {
    return oneHopDao.findRelatedEntities(opContext, graphFilters, offset, count);
  }

  @Nonnull
  @Override
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    return lineageDao.getLineage(opContext, entityUrn, lineageGraphFilters, offset, count, maxHops);
  }

  @Nonnull
  @Override
  public EntityLineageResult getImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int maxHops) {
    return lineageDao.getImpactLineage(opContext, entityUrn, lineageGraphFilters, maxHops);
  }

  @Override
  public void removeNode(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (!writable) {
      return;
    }
    writeSink.removeNode(opContext, urn);
  }

  @Override
  public void removeEdgesFromNode(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull com.linkedin.metadata.query.filter.RelationshipFilter relationshipFilter) {
    if (!writable) {
      return;
    }
    writeSink.removeEdgesFromNode(opContext, urn, relationshipTypes, relationshipFilter);
  }

  @Override
  public void configure() {}

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    if (!writable) {
      return;
    }
    writeSink.clear();
  }

  @Override
  public boolean supportsMultiHop() {
    return true;
  }

  @Override
  public void setEdgeStatus(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      boolean removed,
      @Nonnull EdgeUrnType... edgeUrnTypes) {
    if (!writable) {
      return;
    }
    writeSink.setEdgeStatus(urn, removed, edgeUrnTypes);
  }

  @Nonnull
  @Override
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return oneHopDao.scrollRelatedEntities(opContext, graphFilters, scrollId, count);
  }

  @Override
  public List<Map<String, Object>> raw(
      OperationContext opContext, List<GraphService.EdgeTuple> edgeTuples) {
    return oneHopDao.raw(edgeTuples);
  }
}
