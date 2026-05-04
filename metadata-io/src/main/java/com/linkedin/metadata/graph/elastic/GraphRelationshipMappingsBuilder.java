package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphRelationshipMappingsBuilder {

  private GraphRelationshipMappingsBuilder() {}

  /**
   * Create mapping based on search engine type.
   *
   * @param engineType the target search engine.
   */
  public static Map<String, Object> getMappings(@Nullable SearchEngineType engineType) {
    boolean explicitObjectType = engineType != null && engineType.requiresEs8JavaClient();
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(EDGE_FIELD_SOURCE, getMappingsForEntity(explicitObjectType));
    mappings.put(EDGE_FIELD_DESTINATION, getMappingsForEntity(explicitObjectType));
    mappings.put(EDGE_FIELD_RELNSHIP_TYPE, getMappingsForKeyword());
    mappings.put(EDGE_FIELD_PROPERTIES, getMappingsForEdgeProperties(explicitObjectType));
    mappings.put(EDGE_FIELD_LIFECYCLE_OWNER, getMappingsForKeyword());
    mappings.put(EDGE_FIELD_VIA, getMappingsForKeyword());
    mappings.put(EDGE_FIELD_LIFECYCLE_OWNER_STATUS, getMappingsForBoolean());
    mappings.put(EDGE_FIELD_VIA_STATUS, getMappingsForBoolean());
    // Timestamp and actor fields used for lineage filtering and sorting
    mappings.put("createdOn", getMappingsForLong());
    mappings.put("createdActor", getMappingsForKeyword());
    mappings.put("updatedOn", getMappingsForLong());
    mappings.put("updatedActor", getMappingsForKeyword());
    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForKeyword() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> getMappingsForBoolean() {
    return ImmutableMap.<String, Object>builder().put("type", "boolean").build();
  }

  private static Map<String, Object> getMappingsForLong() {
    return ImmutableMap.<String, Object>builder().put("type", "long").build();
  }

  private static Map<String, Object> getMappingsForEntity(boolean explicitObjectType) {

    Map<String, Object> mappings =
        ImmutableMap.<String, Object>builder()
            .put("urn", getMappingsForKeyword())
            .put("entityType", getMappingsForKeyword())
            .put("removed", getMappingsForBoolean())
            .build();

    return explicitObjectType
        ? ImmutableMap.of("type", "object", "properties", mappings)
        : ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForEdgeProperties(boolean explicitObjectType) {

    Map<String, Object> propertyMappings =
        ImmutableMap.<String, Object>builder().put("source", getMappingsForKeyword()).build();

    return explicitObjectType
        ? ImmutableMap.of("type", "object", "properties", propertyMappings)
        : ImmutableMap.of("properties", propertyMappings);
  }
}
