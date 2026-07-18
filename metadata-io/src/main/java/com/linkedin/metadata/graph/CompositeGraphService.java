package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.graph.write.GraphWriteSink;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

/**
 * Graph {@link GraphService} that delegates reads and primary mutations to a backing implementation
 * (Elasticsearch or Neo4j) and fans out the same mutations to zero or more {@link GraphWriteSink}
 * implementations (e.g. PostgreSQL pgRouting). Elasticsearch and Neo4j persistence live in their
 * respective {@link GraphService} delegates; additional stores implement {@link GraphWriteSink}
 * only.
 */
@RequiredArgsConstructor
public class CompositeGraphService implements GraphService {

  @Nonnull private final GraphService delegate;
  @Nonnull private final List<GraphWriteSink> persistenceBackends;

  @Override
  public GraphServiceConfiguration getGraphServiceConfig() {
    return delegate.getGraphServiceConfig();
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return delegate.getLineageRegistry();
  }

  @Override
  public void addEdge(@Nonnull final OperationContext opContext, @Nonnull final Edge edge) {
    delegate.addEdge(opContext, edge);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.addEdge(edge);
    }
  }

  @Override
  public void upsertEdge(@Nonnull final OperationContext opContext, @Nonnull final Edge edge) {
    delegate.upsertEdge(opContext, edge);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.addEdge(edge);
    }
  }

  @Override
  public void removeEdge(@Nonnull final OperationContext opContext, @Nonnull final Edge edge) {
    delegate.removeEdge(opContext, edge);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.removeEdge(edge);
    }
  }

  @Override
  public void addEdges(@Nonnull final OperationContext opContext, @Nonnull final List<Edge> edges) {
    delegate.addEdges(opContext, edges);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.addEdges(edges);
    }
  }

  @Override
  public void upsertEdges(
      @Nonnull final OperationContext opContext, @Nonnull final List<Edge> edges) {
    delegate.upsertEdges(opContext, edges);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.addEdges(edges);
    }
  }

  @Override
  public void removeEdges(
      @Nonnull final OperationContext opContext, @Nonnull final List<Edge> edges) {
    delegate.removeEdges(opContext, edges);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.removeEdges(edges);
    }
  }

  @Nonnull
  @Override
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      @Nullable final Integer count) {
    return delegate.findRelatedEntities(opContext, graphFilters, offset, count);
  }

  @Nonnull
  @Override
  public EntityLineageResult getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final LineageGraphFilters graphFilters,
      final int offset,
      @Nullable final Integer count,
      final int maxHops) {
    return delegate.getLineage(opContext, entityUrn, graphFilters, offset, count, maxHops);
  }

  @Nonnull
  @Override
  public EntityLineageResult getImpactLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final LineageGraphFilters graphFilters,
      final int maxHops) {
    return delegate.getImpactLineage(opContext, entityUrn, graphFilters, maxHops);
  }

  @Override
  public void removeNode(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    delegate.removeNode(opContext, urn);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.removeNode(opContext, urn);
    }
  }

  @Override
  public void removeEdgesFromNode(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {
    delegate.removeEdgesFromNode(opContext, urn, relationshipTypes, relationshipFilter);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.removeEdgesFromNode(opContext, urn, relationshipTypes, relationshipFilter);
    }
  }

  @Override
  public void configure() {
    delegate.configure();
  }

  @Override
  public void clear(@Nonnull final OperationContext opContext) {
    delegate.clear(opContext);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.clear();
    }
  }

  @Override
  public boolean supportsMultiHop() {
    return delegate.supportsMultiHop();
  }

  @Override
  public void setEdgeStatus(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      final boolean removed,
      @Nonnull final EdgeUrnType... edgeUrnTypes) {
    delegate.setEdgeStatus(opContext, urn, removed, edgeUrnTypes);
    for (GraphWriteSink backend : persistenceBackends) {
      backend.setEdgeStatus(urn, removed, edgeUrnTypes);
    }
  }

  @Nonnull
  @Override
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      @Nonnull final List<SortCriterion> sortCriteria,
      @Nullable final String scrollId,
      @Nullable final String keepAlive,
      @Nullable final Integer count,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    return delegate.scrollRelatedEntities(
        opContext,
        graphFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        count,
        startTimeMillis,
        endTimeMillis);
  }

  @Override
  public List<Map<String, Object>> raw(
      final OperationContext opContext, final List<GraphService.EdgeTuple> edgeTuples) {
    return delegate.raw(opContext, edgeTuples);
  }
}
