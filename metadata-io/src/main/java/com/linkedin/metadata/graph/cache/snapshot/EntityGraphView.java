package com.linkedin.metadata.graph.cache.snapshot;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nonnull;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.AsUndirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.EdgeReversedGraph;

public class EntityGraphView {

  private final List<DirectedEdge> edges;
  private volatile DirectedMultigraph<String, DirectedEdge> forwardGraph;
  private volatile Graph<String, DirectedEdge> reverseGraph;
  private volatile ConnectivityInspector<String, DirectedEdge> connectivityInspector;

  public EntityGraphView(@Nonnull List<DirectedEdge> edges) {
    this.edges = List.copyOf(edges);
  }

  @Nonnull
  public static EntityGraphView fromComponents(@Nonnull Collection<EntityGraphView> components) {
    if (components.size() == 1) {
      return components.iterator().next();
    }
    return new EntityGraphView(unionEdges(components));
  }

  @Nonnull
  public static List<DirectedEdge> unionEdges(@Nonnull Collection<EntityGraphView> components) {
    Map<String, DirectedEdge> deduped = new LinkedHashMap<>();
    for (EntityGraphView component : components) {
      for (DirectedEdge edge : component.getEdges()) {
        deduped.putIfAbsent(edge.canonicalLine(), edge);
      }
    }
    return List.copyOf(deduped.values());
  }

  @Nonnull
  public List<DirectedEdge> getEdges() {
    return edges;
  }

  /**
   * Returns a view with all edges incident on {@code entityUrn} removed. Empty when the vertex was
   * absent; empty edge list when the last edges were removed.
   */
  @Nonnull
  public Optional<EntityGraphView> withoutVertex(@Nonnull String entityUrn) {
    DirectedMultigraph<String, DirectedEdge> graph = forwardGraph();
    if (!graph.containsVertex(entityUrn)) {
      return Optional.empty();
    }
    DirectedMultigraph<String, DirectedEdge> mutableCopy = copyGraph(graph);
    mutableCopy.removeVertex(entityUrn);
    if (mutableCopy.edgeSet().isEmpty()) {
      return Optional.of(new EntityGraphView(List.of()));
    }
    return Optional.of(new EntityGraphView(List.copyOf(mutableCopy.edgeSet())));
  }

  @Nonnull
  public Set<String> expand(
      @Nonnull TraversalDirection direction, @Nonnull Set<String> seeds, int limit, int maxDepth) {
    return expandWithResult(direction, seeds, limit, maxDepth).getVertices();
  }

  @Nonnull
  public ExpandResult expandWithResult(
      @Nonnull TraversalDirection direction, @Nonnull Set<String> seeds, int limit, int maxDepth) {
    Graph<String, DirectedEdge> graph =
        direction == TraversalDirection.FORWARD ? forwardGraph() : reverseGraph();
    Set<String> result = new LinkedHashSet<>();
    Set<String> visited = new HashSet<>();

    for (String seed : seeds) {
      if (graph.containsVertex(seed)) {
        visited.add(seed);
        result.add(seed);
      }
    }

    if (maxDepth <= 0 || result.isEmpty()) {
      return new ExpandResult(result, false, false);
    }

    Queue<String> queue = new ArrayDeque<>(result);
    int depth = 0;
    int levelSize = queue.size();
    int processedAtLevel = 0;

    while (!queue.isEmpty() && result.size() < limit && depth < maxDepth) {
      String current = queue.poll();
      processedAtLevel++;
      for (DirectedEdge edge : graph.outgoingEdgesOf(current)) {
        String neighbor = neighborUrn(edge, direction);
        if (visited.add(neighbor)) {
          result.add(neighbor);
          if (result.size() >= limit) {
            return new ExpandResult(result, false, true);
          }
          queue.add(neighbor);
        }
      }
      if (processedAtLevel == levelSize) {
        depth++;
        levelSize = queue.size();
        processedAtLevel = 0;
      }
    }
    boolean truncatedByMaxDepth = !queue.isEmpty();
    return new ExpandResult(result, truncatedByMaxDepth, false);
  }

  /**
   * Typed neighbor listing for membership-style reads. {@code direction} is relative to stored
   * edges: {@link TraversalDirection#FORWARD} follows source→destination; {@link
   * TraversalDirection#REVERSE} follows destination→source.
   */
  @Nonnull
  public NeighborResult neighborsWithResult(
      @Nonnull TraversalDirection direction,
      @Nonnull String seedUrn,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    if (maxDepth <= 0 || maxDepth > 1 || relationshipTypes.isEmpty() || count <= 0) {
      return new NeighborResult(List.of(), 0);
    }
    if (!containsVertex(seedUrn)) {
      return new NeighborResult(List.of(), 0);
    }

    Graph<String, DirectedEdge> graph =
        direction == TraversalDirection.FORWARD ? forwardGraph() : reverseGraph();
    List<DirectedEdge> matches = new ArrayList<>();
    for (DirectedEdge edge : graph.outgoingEdgesOf(seedUrn)) {
      if (relationshipTypes.contains(edge.getRelationshipType())) {
        matches.add(edge);
      }
    }

    matches.sort(
        Comparator.comparing(DirectedEdge::getRelationshipType)
            .thenComparing(edge -> neighborUrn(edge, direction)));

    int total = matches.size();
    int resolvedStart = Math.max(start, 0);
    if (resolvedStart >= total) {
      return new NeighborResult(List.of(), total);
    }
    int end = Math.min(resolvedStart + count, total);
    return new NeighborResult(matches.subList(resolvedStart, end), total);
  }

  public static final class NeighborResult {
    private final List<DirectedEdge> neighbors;
    private final int total;

    public NeighborResult(@Nonnull List<DirectedEdge> neighbors, int total) {
      this.neighbors = List.copyOf(neighbors);
      this.total = total;
    }

    @Nonnull
    public List<DirectedEdge> getNeighbors() {
      return neighbors;
    }

    public int getTotal() {
      return total;
    }
  }

  public static final class ExpandResult {
    private final Set<String> vertices;
    private final boolean truncatedByMaxDepth;
    private final boolean truncatedByLimit;

    public ExpandResult(
        @Nonnull Set<String> vertices, boolean truncatedByMaxDepth, boolean truncatedByLimit) {
      this.vertices = Set.copyOf(vertices);
      this.truncatedByMaxDepth = truncatedByMaxDepth;
      this.truncatedByLimit = truncatedByLimit;
    }

    @Nonnull
    public Set<String> getVertices() {
      return vertices;
    }

    public boolean isTruncatedByMaxDepth() {
      return truncatedByMaxDepth;
    }

    public boolean isTruncatedByLimit() {
      return truncatedByLimit;
    }
  }

  /**
   * Ordered ancestors along stored forward edges (child → parents for {@code IsPartOf}), up to
   * {@code maxDepth} levels. Does not include {@code seed}. When multiple parents exist, returns
   * all ancestors within depth in breadth-first order (sorted within each level).
   */
  @Nonnull
  public List<String> orderedForwardAncestors(@Nonnull String seed, int maxDepth) {
    if (maxDepth <= 0 || !containsVertex(seed)) {
      return List.of();
    }
    Graph<String, DirectedEdge> graph = forwardGraph();
    List<String> ancestors = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    visited.add(seed);
    List<String> currentLevel = new ArrayList<>();
    for (DirectedEdge edge : graph.outgoingEdgesOf(seed)) {
      String parent = edge.getDestinationUrn();
      if (visited.add(parent)) {
        currentLevel.add(parent);
      }
    }
    int depth = 1;
    while (!currentLevel.isEmpty() && depth <= maxDepth) {
      List<String> sortedLevel = new ArrayList<>(currentLevel);
      sortedLevel.sort(String::compareTo);
      ancestors.addAll(sortedLevel);
      List<String> nextLevel = new ArrayList<>();
      for (String vertex : sortedLevel) {
        for (DirectedEdge edge : graph.outgoingEdgesOf(vertex)) {
          String parent = edge.getDestinationUrn();
          if (visited.add(parent)) {
            nextLevel.add(parent);
          }
        }
      }
      currentLevel = nextLevel;
      depth++;
    }
    return ancestors;
  }

  public boolean containsAllSeeds(@Nonnull Set<String> seeds) {
    Graph<String, DirectedEdge> graph = forwardGraph();
    for (String seed : seeds) {
      if (!graph.containsVertex(seed)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether {@code urn} is a vertex in this cached edge set. */
  public boolean containsVertex(@Nonnull String urn) {
    return forwardGraph().containsVertex(urn);
  }

  public boolean seedsInSameWeakComponent(@Nonnull Set<String> seeds) {
    if (seeds.isEmpty() || !containsAllSeeds(seeds)) {
      return false;
    }
    Set<String> component = connectivityInspector().connectedSetOf(seeds.iterator().next());
    return component.containsAll(seeds);
  }

  @Nonnull
  public List<DirectedEdge> inducedComponentEdges(@Nonnull Set<String> seeds) {
    if (seeds.isEmpty() || !containsAllSeeds(seeds)) {
      return List.of();
    }
    Set<String> componentVertices = connectivityInspector().connectedSetOf(seeds.iterator().next());
    Graph<String, DirectedEdge> induced = new AsSubgraph<>(forwardGraph(), componentVertices);
    return List.copyOf(induced.edgeSet());
  }

  @Nonnull
  public String componentFingerprint(@Nonnull Set<String> seeds) {
    return EntityGraphSnapshotBuilder.topologyFingerprint(inducedComponentEdges(seeds));
  }

  public int vertexCount() {
    return forwardGraph().vertexSet().size();
  }

  public int edgeCount() {
    return edges.size();
  }

  @Nonnull
  private static String neighborUrn(
      @Nonnull DirectedEdge edge, @Nonnull TraversalDirection direction) {
    return direction == TraversalDirection.FORWARD ? edge.getDestinationUrn() : edge.getSourceUrn();
  }

  @Nonnull
  private DirectedMultigraph<String, DirectedEdge> forwardGraph() {
    DirectedMultigraph<String, DirectedEdge> graph = forwardGraph;
    if (graph == null) {
      synchronized (this) {
        graph = forwardGraph;
        if (graph == null) {
          graph = buildForwardGraph();
          forwardGraph = graph;
        }
      }
    }
    return graph;
  }

  @Nonnull
  private Graph<String, DirectedEdge> reverseGraph() {
    Graph<String, DirectedEdge> graph = reverseGraph;
    if (graph == null) {
      synchronized (this) {
        graph = reverseGraph;
        if (graph == null) {
          graph = new EdgeReversedGraph<>(forwardGraph());
          reverseGraph = graph;
        }
      }
    }
    return graph;
  }

  @Nonnull
  private ConnectivityInspector<String, DirectedEdge> connectivityInspector() {
    ConnectivityInspector<String, DirectedEdge> inspector = connectivityInspector;
    if (inspector == null) {
      synchronized (this) {
        inspector = connectivityInspector;
        if (inspector == null) {
          inspector = new ConnectivityInspector<>(new AsUndirectedGraph<>(forwardGraph()));
          connectivityInspector = inspector;
        }
      }
    }
    return inspector;
  }

  @Nonnull
  private DirectedMultigraph<String, DirectedEdge> buildForwardGraph() {
    DirectedMultigraph<String, DirectedEdge> graph = new DirectedMultigraph<>(DirectedEdge.class);
    for (DirectedEdge edge : edges) {
      graph.addVertex(edge.getSourceUrn());
      graph.addVertex(edge.getDestinationUrn());
      if (!graph.containsEdge(edge)) {
        graph.addEdge(edge.getSourceUrn(), edge.getDestinationUrn(), edge);
      }
    }
    return graph;
  }

  @Nonnull
  private static DirectedMultigraph<String, DirectedEdge> copyGraph(
      @Nonnull DirectedMultigraph<String, DirectedEdge> source) {
    DirectedMultigraph<String, DirectedEdge> copy = new DirectedMultigraph<>(DirectedEdge.class);
    for (String vertex : source.vertexSet()) {
      copy.addVertex(vertex);
    }
    for (DirectedEdge edge : source.edgeSet()) {
      copy.addEdge(source.getEdgeSource(edge), source.getEdgeTarget(edge), edge);
    }
    return copy;
  }
}
