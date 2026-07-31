package com.linkedin.metadata.config.entitygraph;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityGraphCacheProperties {

  public static final String CONFIG_JSON_ENV =
      EntityGraphCacheConfigLoader.ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV;
  public static final String DEFAULT_CONFIG_FILE_PATH = "entity-graph-cache.yaml";
  public static final String SNAPSHOTS_MAP_PREFIX = "entityGraphSnapshots.";

  /** PARTIAL graph ids must not collide with {@link #FULL_SNAPSHOTS_MAP}. */
  public static final String RESERVED_PARTIAL_GRAPH_ID = "full";

  public static final String FULL_SNAPSHOTS_MAP = SNAPSHOTS_MAP_PREFIX + RESERVED_PARTIAL_GRAPH_ID;
  public static final String STATUS_MAP = "entityGraphStatus";
  public static final String INVALIDATION_GENERATION_MAP = "entityGraphInvalidationGeneration";

  @Nonnull
  public static String partialSnapshotsMapName(@Nonnull String graphId) {
    return SNAPSHOTS_MAP_PREFIX + graphId;
  }

  private boolean enabled;
  private ConfigFile configFile;
  private Eviction eviction;

  /**
   * Optional JSON overlay (graphs or partial entityGraphCache fragment). Bound from {@link
   * EntityGraphCacheConfigLoader#ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV} via application.yaml; merged
   * after the config file at startup.
   */
  private String configJson;

  private Map<String, GraphDefinition> graphs;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ConfigFile {
    private boolean enabled;
    private String path;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Eviction {
    private LocalEviction local;
    private MemoryPressure memoryPressure;
    private HazelcastEviction hazelcast;
    private ScopeNearCache nearCache;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LocalEviction {
    private boolean enabled;
    private int maxViews;
    private long maxEstimatedBytes;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class MemoryPressure {
    private boolean enabled;
    private int checkIntervalSeconds;
    private int heapUsageThresholdPercent;
    private String action;
    private int cooldownSeconds;

    /**
     * Heap must fall below {@code heapUsageThresholdPercent - hysteresisPercent} to exit pressure.
     */
    private int hysteresisPercent;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class HazelcastEviction {
    private String evictionPolicy;
    private int maxSizePerNode;
    private String maxSizePolicy;
    private int heapMaxSizePercent;
    private int ttlSeconds;
    private int backupCount;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class NearCache {
    private boolean enabled;
    private int maxSize;
  }

  /** Hazelcast near-cache settings split by graph scope (FULL vs PARTIAL snapshot maps). */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ScopeNearCache {
    private NearCache full;

    /**
     * Default off — PARTIAL component snapshots are typically large; enable per-graph if needed.
     */
    private NearCache partial;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphDefinition {
    private boolean enabled;

    /**
     * Snapshot build authority: {@code primary} (aspect lookup), {@code graph} (GraphRetriever
     * scroll), or {@code search} (search index scroll).
     */
    private String buildSource;

    private List<GraphEdgeTripletConfig> edges;
    private LineageEdgeConfig lineage;

    /** Triplet-mode hierarchy shorthand — mutually exclusive with edges.lineage */
    private List<String> entityTypes;

    private String relationshipType;
    private ScopeConfig scope;
    private GraphPopulation population;
    private GraphBounds bounds;
    private ScrollConfig scroll;
    private GraphBindings bindings;
    private GraphEviction eviction;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphEdgeTripletConfig {
    private String sourceEntityType;
    private String destinationEntityType;
    private String relationshipType;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LineageEdgeConfig {
    private List<String> entityTypes;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ScopeConfig {
    private ScopeMode mode;
    private int maxDepth;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphPopulation {
    private PopulationStrategy strategy;

    /**
     * LAZY: snapshot staleness and {@code COOLDOWN} retry interval. SCHEDULED: background rebuild
     * interval (FULL scope only).
     */
    @JsonAlias("maxStaleSeconds")
    private int intervalSeconds;

    /** FULL + LAZY only: {@code SYNC} blocks expand on rebuild; {@code BACKGROUND} fail-closed. */
    private RebuildExecution rebuildExecution;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphBounds {
    private int maxVertices;
    private Integer maxEdges;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ScrollConfig {
    private int batchSize;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphBindings {
    private List<String> filterFields;
    private List<String> policyFieldTypes;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GraphEviction {
    private LocalEviction local;

    /**
     * Optional per-graph near cache for {@code scope.mode: PARTIAL} (each graph has its own {@code
     * entityGraphSnapshots.<graphId>} map). Ignored for {@code scope.mode: FULL} — use {@link
     * Eviction#getNearCache()}{@code .full} instead.
     */
    private NearCache nearCache;
  }

  public enum ScopeMode {
    FULL,
    PARTIAL
  }

  public enum PopulationStrategy {
    SCHEDULED,
    LAZY
  }

  public enum RebuildExecution {
    SYNC,
    BACKGROUND
  }
}
