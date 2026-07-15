package com.linkedin.metadata.graph.write;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Additional graph persistence for the same mutations as the primary {@link
 * com.linkedin.metadata.graph.GraphService} (Elasticsearch or Neo4j). Implementations include
 * PostgreSQL pgRouting SqlSetup tables; the primary store does not implement this interface.
 *
 * <p>Multiple backends are composed by {@link com.linkedin.metadata.graph.CompositeGraphService}.
 *
 * <p>Each implementation chooses its own document / vertex id scheme (for example {@code
 * elasticsearch.idHashAlgo} applies only to Elasticsearch, not to other sinks).
 */
public interface GraphWriteSink {

  GraphWriteSink NOOP = new GraphWriteSink() {};

  default void addEdge(@Nonnull Edge edge) {}

  /** Same as repeated {@link #addEdge(Edge)}; implementations may use JDBC or bulk APIs. */
  default void addEdges(@Nonnull List<Edge> edges) {
    for (Edge edge : edges) {
      addEdge(edge);
    }
  }

  default void removeEdge(@Nonnull Edge edge) {}

  /** Same as repeated {@link #removeEdge(Edge)}. */
  default void removeEdges(@Nonnull List<Edge> edges) {
    for (Edge edge : edges) {
      removeEdge(edge);
    }
  }

  default void removeNode(@Nonnull OperationContext opContext, @Nonnull Urn urn) {}

  default void removeEdgesFromNode(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter) {}

  default void setEdgeStatus(
      @Nonnull Urn urn, boolean removed, @Nonnull EdgeUrnType... edgeUrnTypes) {}

  default void clear() {}
}
