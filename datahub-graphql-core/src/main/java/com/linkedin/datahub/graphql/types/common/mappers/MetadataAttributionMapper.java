package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MetadataAttribution;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MetadataAttributionMapper
    implements ModelMapper<com.linkedin.common.MetadataAttribution, MetadataAttribution> {

  public static final MetadataAttributionMapper INSTANCE = new MetadataAttributionMapper();

  public static MetadataAttribution map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.MetadataAttribution metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public MetadataAttribution apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.MetadataAttribution input) {
    final MetadataAttribution result = new MetadataAttribution();
    result.setTime(input.getTime());
    result.setActor(UrnToEntityMapper.map(context, input.getActor()));
    if (input.getSource() != null) {
      result.setSource(UrnToEntityMapper.map(context, input.getSource()));
    }
    if (input.getSourceDetail() != null) {
      result.setSourceDetail(StringMapMapper.map(context, input.getSourceDetail()));
    }
    return result;
  }
}
