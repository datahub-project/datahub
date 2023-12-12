package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.DownstreamEntityRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DownstreamEntityRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, DownstreamEntityRelationships> {

  public static final DownstreamEntityRelationshipsMapper INSTANCE =
      new DownstreamEntityRelationshipsMapper();

  public static DownstreamEntityRelationships map(
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(relationships);
  }

  @Override
  public DownstreamEntityRelationships apply(
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final DownstreamEntityRelationships result = new DownstreamEntityRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(EntityRelationshipLegacyMapper::map)
            .collect(Collectors.toList()));
    return result;
  }
}
