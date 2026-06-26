package com.linkedin.metadata.config.entitygraph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.LineageEdgeConfig;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Slf4j
public class EntityGraphCacheConfigLoader {

  /**
   * JSON overlay env var — merged after Spring defaults and optional config file (see rate
   * limiter).
   */
  public static final String ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV = "ENTITY_GRAPH_CACHE_CONFIG_JSON";

  private final ObjectMapper jsonMapper;
  private final ObjectMapper yamlMapper;

  public EntityGraphCacheConfigLoader(ObjectMapper jsonMapper, ObjectMapper yamlMapper) {
    this.jsonMapper = jsonMapper;
    this.yamlMapper = yamlMapper;
  }

  public EntityGraphCacheProperties loadEffective(EntityGraphCacheProperties fromSpring) {
    EntityGraphCacheProperties effective = deepCopy(fromSpring);
    ensureNestedDefaults(effective);

    if (effective.getConfigFile() != null
        && effective.getConfigFile().isEnabled()
        && StringUtils.hasText(effective.getConfigFile().getPath())) {
      applyFileOverlay(effective.getConfigFile().getPath(), effective);
    }

    String jsonOverlay = resolveJsonOverlay(effective);
    if (StringUtils.hasText(jsonOverlay)) {
      applyJsonOverlay(jsonOverlay, effective, ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV);
    }

    validate(effective);
    return effective;
  }

  /**
   * JSON overlay from Spring-bound {@code entityGraphCache.configJson} (populated from env in
   * application.yaml). Falls back to direct env read for callers that bypass Spring binding.
   */
  private static String resolveJsonOverlay(EntityGraphCacheProperties effective) {
    if (StringUtils.hasText(effective.getConfigJson())) {
      return effective.getConfigJson();
    }
    return System.getenv(ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV);
  }

  /**
   * Applies a JSON overlay without reading env vars. Accepts either a top-level {@code graphs:}
   * document or a fragment wrapped in {@code entityGraphCache:} (same shape as {@link
   * #ENTITY_GRAPH_CACHE_CONFIG_JSON_ENV}).
   */
  public void applyJsonOverlay(String json, EntityGraphCacheProperties target, String sourceLabel) {
    try {
      JsonNode root = jsonMapper.readTree(json);
      JsonNode node = root.has("entityGraphCache") ? root.get("entityGraphCache") : root;
      mergeOverlayNode(node, target, sourceLabel);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to parse entity graph cache overlay from " + sourceLabel, e);
    }
  }

  private void applyFileOverlay(String path, EntityGraphCacheProperties target) {
    try (InputStream stream = openConfigStream(path)) {
      JsonNode root = yamlMapper.readTree(stream);
      JsonNode node = root.has("entityGraphCache") ? root.get("entityGraphCache") : root;
      mergeOverlayNode(node, target, path);
    } catch (FileNotFoundException e) {
      throw new IllegalStateException(
          "Entity graph cache configuration file was NOT found at " + path, e);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to load entity graph cache configuration from: " + path, e);
    }
  }

  private InputStream openConfigStream(String path) throws IOException {
    ClassPathResource classpathResource = new ClassPathResource(path);
    if (classpathResource.exists()) {
      return classpathResource.getInputStream();
    }
    FileSystemResource filesystemResource = new FileSystemResource(path);
    if (!filesystemResource.exists()) {
      throw new FileNotFoundException(path);
    }
    return filesystemResource.getInputStream();
  }

  private void mergeOverlayNode(
      JsonNode overlay, EntityGraphCacheProperties target, String sourceLabel) throws IOException {
    if (overlay == null || overlay.isNull() || overlay.isMissingNode()) {
      return;
    }
    ensureNestedDefaults(target);

    if (overlay.has("eviction")) {
      mergeEvictionOverlay(overlay.get("eviction"), target);
    }

    if (overlay instanceof ObjectNode) {
      ObjectNode remainder = ((ObjectNode) overlay).deepCopy();
      remainder.remove("eviction");
      if (remainder.size() > 0) {
        jsonMapper.readerForUpdating(target).readValue(remainder);
      }
    }

    log.info("Applied entity graph cache overlay from {}", sourceLabel);
  }

  private void mergeEvictionOverlay(JsonNode evictionNode, EntityGraphCacheProperties target)
      throws IOException {
    if (evictionNode == null || evictionNode.isNull()) {
      return;
    }
    if (target.getEviction() == null) {
      target.setEviction(new EntityGraphCacheProperties.Eviction());
    }
    EntityGraphCacheProperties.Eviction eviction = target.getEviction();

    mergeEvictionSubsection(
        evictionNode,
        "nearCache",
        eviction.getNearCache(),
        EntityGraphCacheProperties.ScopeNearCache.class,
        eviction::setNearCache);
    mergeEvictionSubsection(
        evictionNode,
        "local",
        eviction.getLocal(),
        EntityGraphCacheProperties.LocalEviction.class,
        eviction::setLocal);
    mergeEvictionSubsection(
        evictionNode,
        "memoryPressure",
        eviction.getMemoryPressure(),
        EntityGraphCacheProperties.MemoryPressure.class,
        eviction::setMemoryPressure);
    mergeEvictionSubsection(
        evictionNode,
        "hazelcast",
        eviction.getHazelcast(),
        EntityGraphCacheProperties.HazelcastEviction.class,
        eviction::setHazelcast);
  }

  private <T> void mergeEvictionSubsection(
      JsonNode evictionNode, String fieldName, T current, Class<T> type, Consumer<T> setter)
      throws IOException {
    JsonNode node = evictionNode.get(fieldName);
    if (node == null || node.isNull()) {
      return;
    }
    T merged =
        current != null
            ? jsonMapper.readerForUpdating(current).readValue(node)
            : jsonMapper.treeToValue(node, type);
    setter.accept(merged);
  }

  private void ensureNestedDefaults(EntityGraphCacheProperties target) {
    if (target.getGraphs() == null) {
      target.setGraphs(new HashMap<>());
    }
  }

  private void validateTopLevel(EntityGraphCacheProperties config, List<String> errors) {
    if (!config.isEnabled()) {
      return;
    }
    if (config.getEviction() == null) {
      errors.add(
          "entityGraphCache.eviction is required when enabled — configure defaults in "
              + "application.yaml");
      return;
    }
    if (config.getEviction().getLocal() == null) {
      errors.add("entityGraphCache.eviction.local is required when enabled");
    }
    if (config.getEviction().getMemoryPressure() == null) {
      errors.add("entityGraphCache.eviction.memoryPressure is required when enabled");
    } else {
      String action = config.getEviction().getMemoryPressure().getAction();
      if (action != null
          && !"EVICT_ALL_LOCAL".equals(action)
          && !"EVICT_LOCAL_LRU".equals(action)) {
        errors.add(
            "entityGraphCache.eviction.memoryPressure.action must be EVICT_ALL_LOCAL or"
                + " EVICT_LOCAL_LRU");
      }
    }
    if (config.getEviction().getHazelcast() == null) {
      errors.add("entityGraphCache.eviction.hazelcast is required when enabled");
    }
    if (config.getEviction().getNearCache() == null) {
      errors.add(
          "entityGraphCache.eviction.nearCache is required when enabled — configure in "
              + "entity-graph-cache.yaml (or JSON overlay)");
    }
  }

  private EntityGraphCacheProperties deepCopy(EntityGraphCacheProperties source) {
    return jsonMapper.convertValue(source, EntityGraphCacheProperties.class);
  }

  public void validate(EntityGraphCacheProperties config) {
    if (!config.isEnabled()) {
      return;
    }

    ensureNestedDefaults(config);
    List<String> errors = new ArrayList<>();

    validateTopLevel(config, errors);

    for (Map.Entry<String, GraphDefinition> entry : config.getGraphs().entrySet()) {
      validateGraph(entry.getKey(), entry.getValue(), errors);
    }

    validateBindingUniqueness(config, errors);

    if (config.getGraphs().isEmpty()) {
      errors.add("entity graph cache defines no graphs");
    }

    if (!errors.isEmpty()) {
      throw new IllegalStateException(
          "Invalid entity graph cache configuration: " + String.join("; ", errors));
    }
  }

  private void validateGraph(String graphId, GraphDefinition graph, List<String> errors) {
    if (graph == null) {
      errors.add("graph " + graphId + " is null");
      return;
    }

    warnAndDropDeprecatedGraphNearCache(graphId, graph);

    ScopeMode scopeMode = graph.getScope() != null ? graph.getScope().getMode() : ScopeMode.FULL;
    if (scopeMode == ScopeMode.PARTIAL
        && EntityGraphCacheProperties.RESERVED_PARTIAL_GRAPH_ID.equalsIgnoreCase(graphId)) {
      errors.add(
          "graph "
              + graphId
              + ": graphId '"
              + EntityGraphCacheProperties.RESERVED_PARTIAL_GRAPH_ID
              + "' is reserved for FULL snapshots (collides with "
              + EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP
              + ")");
    }

    if (!graph.isEnabled()) {
      return;
    }

    boolean hasLineage = graph.getLineage() != null;
    boolean hasTripletList = !CollectionUtils.isEmpty(graph.getEdges());
    boolean hasTripletShorthand =
        !CollectionUtils.isEmpty(graph.getEntityTypes())
            && StringUtils.hasText(graph.getRelationshipType());

    if (hasLineage && (hasTripletList || hasTripletShorthand)) {
      errors.add("graph " + graphId + ": cannot mix edges.lineage with triplet config");
    }
    if (hasLineage && !CollectionUtils.isEmpty(graph.getEntityTypes()) && hasTripletShorthand) {
      errors.add(
          "graph "
              + graphId
              + ": graph-level entityTypes/relationshipType forbidden with edges.lineage");
    }

    if (!hasLineage && !hasTripletList && !hasTripletShorthand) {
      errors.add(
          "graph " + graphId + ": must define edges[], lineage, or entityTypes+relationshipType");
    }

    if (graph.getPopulation() == null || graph.getPopulation().getStrategy() == null) {
      errors.add("graph " + graphId + ": population.strategy is required");
    }

    if (!StringUtils.hasText(graph.getBuildSource())) {
      errors.add("graph " + graphId + ": buildSource is required (primary, graph, or search)");
    } else {
      String normalized = graph.getBuildSource().trim().toUpperCase(Locale.ROOT);
      if (!"PRIMARY".equals(normalized)
          && !"GRAPH".equals(normalized)
          && !"SEARCH".equals(normalized)) {
        errors.add("graph " + graphId + ": buildSource must be primary, graph, or search");
      }
      if (graph.getScope() != null
          && graph.getScope().getMode() == ScopeMode.FULL
          && "PRIMARY".equals(normalized)) {
        errors.add("graph " + graphId + ": FULL scope requires buildSource graph or search");
      }
      validateBindingBuildSourceRules(graphId, graph, normalized, errors);
    }

    if (graph.getPopulation() != null && graph.getPopulation().getIntervalSeconds() <= 0) {
      errors.add("graph " + graphId + ": population.intervalSeconds must be > 0");
    }

    if (graph.getScope() != null && graph.getScope().getMode() == ScopeMode.PARTIAL) {
      PopulationStrategy strategy =
          graph.getPopulation() != null ? graph.getPopulation().getStrategy() : null;
      if (strategy == PopulationStrategy.SCHEDULED) {
        errors.add("graph " + graphId + ": SCHEDULED + PARTIAL scope is invalid");
      }
      if (graph.getScope().getMaxDepth() <= 0) {
        errors.add("graph " + graphId + ": PARTIAL scope requires scope.maxDepth > 0");
      }
    }

    if (graph.getScope() != null
        && graph.getScope().getMode() == ScopeMode.FULL
        && graph.getScope().getMaxDepth() > 0) {
      errors.add(
          "graph "
              + graphId
              + ": scope.maxDepth is not valid for FULL scope; use bounds.maxVertices/maxEdges");
    }

    if (graph.getPopulation() != null
        && graph.getPopulation().getRebuildExecution() == RebuildExecution.BACKGROUND) {
      if (graph.getPopulation().getStrategy() != PopulationStrategy.LAZY) {
        errors.add("graph " + graphId + ": rebuildExecution BACKGROUND requires LAZY strategy");
      } else if (graph.getScope() == null || graph.getScope().getMode() != ScopeMode.FULL) {
        errors.add("graph " + graphId + ": rebuildExecution BACKGROUND requires FULL scope");
      }
    }

    if (hasLineage) {
      LineageEdgeConfig lineage = graph.getLineage();
      if (lineage.getEntityTypes() != null) {
        for (String key : lineage.getEntityTypes()) {
          if ("directions".equalsIgnoreCase(key) || "relationshipTypes".equalsIgnoreCase(key)) {
            errors.add("graph " + graphId + ": unknown lineage config keys are not supported");
          }
        }
      }
    }
  }

  private static void validateBindingBuildSourceRules(
      String graphId, GraphDefinition graph, String normalizedBuildSource, List<String> errors) {
    if (graph.getBindings() == null) {
      return;
    }

    boolean hasFilterFields = !CollectionUtils.isEmpty(graph.getBindings().getFilterFields());
    boolean hasPolicyFields = !CollectionUtils.isEmpty(graph.getBindings().getPolicyFieldTypes());

    if (hasFilterFields) {
      if (!"SEARCH".equals(normalizedBuildSource)) {
        errors.add(
            "graph " + graphId + ": filterFields requires buildSource search and scope.mode FULL");
      } else if (graph.getScope() == null || graph.getScope().getMode() != ScopeMode.FULL) {
        errors.add("graph " + graphId + ": filterFields requires scope.mode FULL");
      }
    }

    if (hasPolicyFields) {
      if ("PRIMARY".equals(normalizedBuildSource)) {
        // ok
      } else if ("SEARCH".equals(normalizedBuildSource)
          && graph.getScope() != null
          && graph.getScope().getMode() == ScopeMode.FULL) {
        // Bundled domain@search — policy expansion uses the same FULL search snapshot.
      } else {
        errors.add(
            "graph "
                + graphId
                + ": policyFieldTypes requires buildSource primary, or search with scope.mode FULL");
      }
    }

    if ("PRIMARY".equals(normalizedBuildSource) && hasFilterFields) {
      errors.add("graph " + graphId + ": buildSource primary cannot declare filterFields");
    }
  }

  private static void validateBindingUniqueness(
      EntityGraphCacheProperties config, List<String> errors) {
    Map<String, String> filterFieldOwners = new HashMap<>();
    Map<String, String> policyFieldOwners = new HashMap<>();

    for (Map.Entry<String, GraphDefinition> entry : config.getGraphs().entrySet()) {
      String graphId = entry.getKey();
      GraphDefinition graph = entry.getValue();
      if (graph == null || !graph.isEnabled() || graph.getBindings() == null) {
        continue;
      }

      if (graph.getBindings().getFilterFields() != null) {
        for (String field : graph.getBindings().getFilterFields()) {
          registerBinding(
              filterFieldOwners,
              field.toLowerCase(Locale.ROOT),
              graphId,
              "filterFields",
              field,
              errors);
        }
      }
      if (graph.getBindings().getPolicyFieldTypes() != null) {
        for (String field : graph.getBindings().getPolicyFieldTypes()) {
          registerBinding(
              policyFieldOwners,
              field.toUpperCase(Locale.ROOT),
              graphId,
              "policyFieldTypes",
              field,
              errors);
        }
      }
    }
  }

  private void warnAndDropDeprecatedGraphNearCache(
      @Nonnull String graphId, @Nonnull GraphDefinition graph) {
    if (graph.getEviction() == null || graph.getEviction().getNearCache() == null) {
      return;
    }
    ScopeMode mode = graph.getScope() != null ? graph.getScope().getMode() : ScopeMode.FULL;
    if (mode != ScopeMode.FULL) {
      return;
    }
    log.warn(
        "graph {}: graphs.*.eviction.nearCache is ignored for scope.mode FULL; configure "
            + "entityGraphCache.eviction.nearCache.full instead (FULL graphs share "
            + EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP
            + ")",
        graphId);
    graph.getEviction().setNearCache(null);
  }

  private static void registerBinding(
      Map<String, String> owners,
      String normalizedKey,
      String graphId,
      String bindingType,
      String rawValue,
      List<String> errors) {
    String existing = owners.putIfAbsent(normalizedKey, graphId);
    if (existing != null && !existing.equals(graphId)) {
      errors.add(
          "binding "
              + bindingType
              + " '"
              + rawValue
              + "' is claimed by graphs "
              + existing
              + " and "
              + graphId);
    }
  }
}
