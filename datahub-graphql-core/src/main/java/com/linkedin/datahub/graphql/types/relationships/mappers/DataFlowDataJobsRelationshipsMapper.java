package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.DataFlowDataJobsRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataFlowDataJobsRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, DataFlowDataJobsRelationships> {

  public static final DataFlowDataJobsRelationshipsMapper INSTANCE =
      new DataFlowDataJobsRelationshipsMapper();

  public static DataFlowDataJobsRelationships map(
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(relationships);
  }

  @Override
  public DataFlowDataJobsRelationships apply(
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final DataFlowDataJobsRelationships result = new DataFlowDataJobsRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(EntityRelationshipLegacyMapper::map)
            .collect(Collectors.toList()));
    return result;
  }
}
