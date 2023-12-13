package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.UpstreamEntityRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class UpstreamEntityRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, UpstreamEntityRelationships> {

  public static final UpstreamEntityRelationshipsMapper INSTANCE =
      new UpstreamEntityRelationshipsMapper();

  public static UpstreamEntityRelationships map(
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(relationships);
  }

  @Override
  public UpstreamEntityRelationships apply(
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final UpstreamEntityRelationships result = new UpstreamEntityRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(EntityRelationshipLegacyMapper::map)
            .collect(Collectors.toList()));
    return result;
  }
}
