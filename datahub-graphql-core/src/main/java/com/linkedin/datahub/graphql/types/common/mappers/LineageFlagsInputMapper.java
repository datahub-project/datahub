package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityTypeToPlatforms;
import com.linkedin.datahub.graphql.generated.LineageFlags;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps GraphQL SearchFlags to Pegasus
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class LineageFlagsInputMapper
    implements ModelMapper<LineageFlags, com.linkedin.metadata.query.LineageFlags> {

  public static final LineageFlagsInputMapper INSTANCE = new LineageFlagsInputMapper();

  @Nonnull
  public static com.linkedin.metadata.query.LineageFlags map(
      QueryContext queryContext, @Nonnull final LineageFlags lineageFlags) {
    return INSTANCE.apply(queryContext, lineageFlags);
  }

  @Override
  public com.linkedin.metadata.query.LineageFlags apply(
      QueryContext context, @Nullable final LineageFlags lineageFlags) {
    com.linkedin.metadata.query.LineageFlags result =
        new com.linkedin.metadata.query.LineageFlags();
    if (lineageFlags == null) {
      return result;
    }
    if (lineageFlags.getIgnoreAsHops() != null) {
      result.setIgnoreAsHops(mapIgnoreAsHops(lineageFlags.getIgnoreAsHops()));
    }
    if (lineageFlags.getStartTimeMillis() != null) {
      result.setStartTimeMillis(lineageFlags.getStartTimeMillis());
    }
    // Default to "now" if no end time is provided, but start time is provided.
    Long endTimeMillis =
        ResolverUtils.getLineageEndTimeMillis(
            lineageFlags.getStartTimeMillis(), lineageFlags.getEndTimeMillis());
    if (endTimeMillis != null) {
      result.setEndTimeMillis(endTimeMillis);
    }
    if (lineageFlags.getEntitiesExploredPerHopLimit() != null) {
      result.setEntitiesExploredPerHopLimit(lineageFlags.getEntitiesExploredPerHopLimit());
    }
    return result;
  }

  private static UrnArrayMap mapIgnoreAsHops(List<EntityTypeToPlatforms> ignoreAsHops) {
    UrnArrayMap result = new UrnArrayMap();
    ignoreAsHops.forEach(
        ignoreAsHop ->
            result.put(
                EntityTypeMapper.getName(ignoreAsHop.getEntityType()),
                new UrnArray(
                    Optional.ofNullable(ignoreAsHop.getPlatforms())
                        .orElse(Collections.emptyList())
                        .stream()
                        .map(UrnUtils::getUrn)
                        .collect(Collectors.toList()))));
    return result;
  }
}
