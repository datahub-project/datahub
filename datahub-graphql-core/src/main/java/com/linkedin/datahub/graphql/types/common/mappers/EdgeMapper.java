package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityEdge;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps {@link com.linkedin.common.Edge} Pegasus records to the generated GraphQL {@link
 * EntityEdge}.
 */
@Slf4j
public class EdgeMapper {

  private EdgeMapper() {}

  /**
   * Maps a single PDL {@link com.linkedin.common.Edge} to a GraphQL {@link EntityEdge}. Returns
   * {@code null} when the destination URN cannot be resolved (unknown entity type).
   */
  @Nullable
  public static EntityEdge map(
      @Nullable QueryContext context, @Nonnull com.linkedin.common.Edge edge) {
    Entity destination = UrnToEntityMapper.map(context, edge.getDestinationUrn());
    if (destination == null) {
      log.warn(
          "EdgeMapper: could not resolve destination URN {} — skipping edge",
          edge.getDestinationUrn());
      return null;
    }

    EntityEdge result = new EntityEdge();
    result.setDestination(destination);

    if (edge.hasCreated() && edge.getCreated() != null) {
      AuditStamp created = AuditStampMapper.map(context, edge.getCreated());
      result.setCreated(created);
    }
    if (edge.hasLastModified() && edge.getLastModified() != null) {
      AuditStamp lastModified = AuditStampMapper.map(context, edge.getLastModified());
      result.setLastModified(lastModified);
    }
    if (edge.hasProperties() && edge.getProperties() != null) {
      result.setProperties(StringMapMapper.map(context, edge.getProperties()));
    }

    return result;
  }

  /**
   * Maps a list of PDL {@link com.linkedin.common.Edge} objects, drops and warns any whose
   * destination cannot be resolved.
   */
  @Nonnull
  public static List<EntityEdge> mapList(
      @Nullable QueryContext context, @Nullable List<com.linkedin.common.Edge> edges) {
    if (edges == null) {
      return Collections.emptyList();
    }
    return edges.stream()
        .map(e -> map(context, e))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
