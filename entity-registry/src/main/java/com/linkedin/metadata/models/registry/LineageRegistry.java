package com.linkedin.metadata.models.registry;

import com.google.common.collect.Streams;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.commons.lang3.tuple.Triple;

/**
 * The Lineage Registry provides a mechanism to retrieve metadata about the lineage relationships
 * between different entities Lineage relationship denotes whether an entity is directly upstream or
 * downstream of another entity
 */
public class LineageRegistry {

  private final Map<String, LineageSpec> _lineageSpecMap;
  private final EntityRegistry _entityRegistry;

  public LineageRegistry(EntityRegistry entityRegistry) {
    _lineageSpecMap = buildLineageSpecs(entityRegistry);
    _entityRegistry = entityRegistry;
  }

  private Map<String, LineageSpec> buildLineageSpecs(EntityRegistry entityRegistry) {
    // 1. Flatten relationship annotations into a list of lineage edges (source, dest, type,
    // isUpstream)
    Collection<LineageEdge> lineageEdges =
        entityRegistry.getEntitySpecs().entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().getRelationshipFieldSpecs().stream()
                        .flatMap(
                            spec ->
                                getLineageEdgesFromRelationshipAnnotation(
                                    entry.getKey(), spec.getRelationshipAnnotation())))
            // If there are multiple edges with the same source, dest, edge type, get one of them
            .collect(
                Collectors.toMap(
                    edge -> Triple.of(edge.getSourceEntity(), edge.getDestEntity(), edge.getType()),
                    Function.identity(),
                    (x1, x2) -> x1))
            .values();

    // 2. Figure out the upstream and downstream edges of each entity type
    Map<String, Set<EdgeInfo>> upstreamPerEntity = new HashMap<>();
    Map<String, Set<EdgeInfo>> downstreamPerEntity = new HashMap<>();
    // A downstreamOf B : A -> upstream (downstreamOf, OUTGOING), B -> downstream (downstreamOf,
    // INCOMING)
    // A produces B :     A -> downstream (produces, OUTGOING), B -> upstream (produces, INCOMING)
    for (LineageEdge edge : lineageEdges) {
      if (edge.isUpstream()) {
        upstreamPerEntity
            .computeIfAbsent(edge.sourceEntity.toLowerCase(), (k) -> new HashSet<>())
            .add(new EdgeInfo(edge.type, RelationshipDirection.OUTGOING, edge.destEntity));
        downstreamPerEntity
            .computeIfAbsent(edge.destEntity.toLowerCase(), (k) -> new HashSet<>())
            .add(new EdgeInfo(edge.type, RelationshipDirection.INCOMING, edge.sourceEntity));
      } else {
        downstreamPerEntity
            .computeIfAbsent(edge.sourceEntity.toLowerCase(), (k) -> new HashSet<>())
            .add(new EdgeInfo(edge.type, RelationshipDirection.OUTGOING, edge.destEntity));
        upstreamPerEntity
            .computeIfAbsent(edge.destEntity.toLowerCase(), (k) -> new HashSet<>())
            .add(new EdgeInfo(edge.type, RelationshipDirection.INCOMING, edge.sourceEntity));
      }
    }

    return entityRegistry.getEntitySpecs().keySet().stream()
        .collect(
            Collectors.toMap(
                String::toLowerCase,
                entityName ->
                    new LineageSpec(
                        new ArrayList<>(
                            upstreamPerEntity.getOrDefault(
                                entityName.toLowerCase(), Collections.emptySet())),
                        new ArrayList<>(
                            downstreamPerEntity.getOrDefault(
                                entityName.toLowerCase(), Collections.emptySet())))));
  }

  private Stream<LineageEdge> getLineageEdgesFromRelationshipAnnotation(
      String sourceEntity, RelationshipAnnotation annotation) {
    if (!annotation.isLineage()) {
      return Stream.empty();
    }
    return annotation.getValidDestinationTypes().stream()
        .map(
            destEntity ->
                new LineageEdge(
                    sourceEntity, destEntity, annotation.getName(), annotation.isUpstream()));
  }

  public LineageSpec getLineageSpec(String entityName) {
    return _lineageSpecMap.get(entityName.toLowerCase());
  }

  public Set<String> getEntitiesWithLineageToEntityType(String entityType) {
    Map<String, EntitySpec> specs = _entityRegistry.getEntitySpecs();
    Set<String> result =
        Streams.concat(
                _lineageSpecMap.get(entityType.toLowerCase()).getDownstreamEdges().stream(),
                _lineageSpecMap.get(entityType.toLowerCase()).getUpstreamEdges().stream())
            .map(EdgeInfo::getOpposingEntityType)
            .map(entity -> specs.get(entity.toLowerCase()).getName())
            .collect(Collectors.toSet());
    result.add(entityType);
    return result;
  }

  public List<EdgeInfo> getLineageRelationships(String entityName, LineageDirection direction) {
    LineageSpec spec = getLineageSpec(entityName);
    if (spec == null) {
      return Collections.emptyList();
    }

    if (entityName.equals("schemaField")) {
      return getSchemaFieldRelationships(direction);
    }

    if (direction == LineageDirection.UPSTREAM) {
      return spec.getUpstreamEdges();
    }
    return spec.getDownstreamEdges();
  }

  private List<EdgeInfo> getSchemaFieldRelationships(LineageDirection direction) {
    List<EdgeInfo> schemaFieldEdges = new ArrayList<>();
    if (direction == LineageDirection.UPSTREAM) {
      schemaFieldEdges.add(
          new EdgeInfo("DownstreamOf", RelationshipDirection.OUTGOING, "schemafield"));
    } else {
      schemaFieldEdges.add(
          new EdgeInfo("DownstreamOf", RelationshipDirection.INCOMING, "schemafield"));
    }
    return schemaFieldEdges;
  }

  @Value
  private static class LineageEdge {
    String sourceEntity;
    String destEntity;
    String type;
    boolean isUpstream;
  }

  @Value
  public static class LineageSpec {
    List<EdgeInfo> upstreamEdges;
    List<EdgeInfo> downstreamEdges;
  }

  @Value
  public static class EdgeInfo {
    String type;
    RelationshipDirection direction;
    String opposingEntityType;

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }

      if (o instanceof EdgeInfo) {
        return ((EdgeInfo) o).type.equalsIgnoreCase(this.type)
            && ((EdgeInfo) o).direction.equals(this.direction)
            && ((EdgeInfo) o).opposingEntityType.equalsIgnoreCase(this.opposingEntityType);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return ((this.type == null ? 0 : this.type.toLowerCase().hashCode())
          ^ (this.direction == null ? 0 : this.direction.hashCode())
          ^ (this.opposingEntityType == null
              ? 0
              : this.opposingEntityType.toLowerCase().hashCode()));
    }
  }
}
