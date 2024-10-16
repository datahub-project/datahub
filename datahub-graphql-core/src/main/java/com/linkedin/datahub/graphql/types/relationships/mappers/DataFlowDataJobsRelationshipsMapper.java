package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataFlowDataJobsRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataFlowDataJobsRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, DataFlowDataJobsRelationships> {

  public static final DataFlowDataJobsRelationshipsMapper INSTANCE =
      new DataFlowDataJobsRelationshipsMapper();

  public static DataFlowDataJobsRelationships map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(context, relationships);
  }

  @Override
  public DataFlowDataJobsRelationships apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final DataFlowDataJobsRelationships result = new DataFlowDataJobsRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(r -> EntityRelationshipLegacyMapper.map(context, r))
            .collect(Collectors.toList()));
    return result;
  }
}
