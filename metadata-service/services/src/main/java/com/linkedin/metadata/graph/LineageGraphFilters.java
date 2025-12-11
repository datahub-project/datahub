/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.graph;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

@Data
@AllArgsConstructor
public class LineageGraphFilters {

  public static LineageGraphFilters EMPTY =
      new LineageGraphFilters(
          LineageDirection.$UNKNOWN, Set.of(), Set.of(), new ConcurrentHashMap<>());

  public static LineageGraphFilters forEntityType(
      @Nonnull LineageRegistry lineageRegistry,
      @Nonnull String entityType,
      @Nonnull LineageDirection lineageDirection) {
    return withEntityTypes(
        lineageDirection, lineageRegistry.getEntitiesWithLineageToEntityType(entityType));
  }

  public static LineageGraphFilters withEntityTypes(
      @Nonnull LineageDirection lineageDirection, @Nullable Set<String> allowedEntityTypes) {
    return new LineageGraphFilters(
        lineageDirection, allowedEntityTypes, null, new ConcurrentHashMap<>());
  }

  // Which lineage direction
  @Nonnull private LineageDirection lineageDirection;

  // entity types you want to allow in your result set
  @Nullable private Set<String> allowedEntityTypes;

  // relationship types to allow in your result set
  @Nullable private Set<String> allowedRelationshipTypes;

  @Nonnull private ConcurrentHashMap<String, Set<LineageRegistry.EdgeInfo>> edgesPerEntityType;

  /**
   * Get the edge info for the given entity urn and applying filters for related types and
   * relationship types
   *
   * @return set of edge infos
   */
  public Set<LineageRegistry.EdgeInfo> getEdgeInfo(
      LineageRegistry lineageRegistry, String entityType) {

    return edgesPerEntityType.computeIfAbsent(
        entityType,
        key ->
            lineageRegistry.getLineageRelationships(entityType, lineageDirection).stream()
                .map(edgeInfo -> fixEntityTypeName(lineageRegistry.getEntityRegistry(), edgeInfo))
                .filter(
                    edgeInfo ->
                        (allowedEntityTypes == null
                                || allowedEntityTypes.isEmpty()
                                || allowedEntityTypes.contains(edgeInfo.getOpposingEntityType()))
                            && (allowedRelationshipTypes == null
                                || allowedRelationshipTypes.isEmpty()
                                || allowedRelationshipTypes.contains(edgeInfo.getType())))
                .collect(Collectors.toSet()));
  }

  public Stream<Pair<String, LineageRegistry.EdgeInfo>> streamEdgeInfo() {
    return edgesPerEntityType.entrySet().stream()
        .flatMap(
            entry -> entry.getValue().stream().map(edgeInfo -> Pair.of(entry.getKey(), edgeInfo)));
  }

  public boolean containsEdgeInfo(String entityType, LineageRegistry.EdgeInfo edgeInfo) {
    return edgesPerEntityType.getOrDefault(entityType, Set.of()).contains(edgeInfo);
  }

  private static LineageRegistry.EdgeInfo fixEntityTypeName(
      EntityRegistry entityRegistry, LineageRegistry.EdgeInfo edgeInfo) {
    String specName = entityRegistry.getEntitySpec(edgeInfo.getOpposingEntityType()).getName();
    if (edgeInfo.getOpposingEntityType().equals(specName)) {
      return edgeInfo;
    } else {
      return new LineageRegistry.EdgeInfo(edgeInfo.getType(), edgeInfo.getDirection(), specName);
    }
  }
}
