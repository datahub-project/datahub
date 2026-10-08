package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.MetadataAttribution;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MetadataAttributionMapper
    implements ModelMapper<com.linkedin.common.MetadataAttribution, MetadataAttribution> {

  public static final MetadataAttributionMapper INSTANCE = new MetadataAttributionMapper();

  @Nullable
  public static MetadataAttribution map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.MetadataAttribution metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  @Nullable
  public MetadataAttribution apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.MetadataAttribution input) {
    final Entity actor = UrnToEntityMapper.map(context, input.getActor());
    if (actor == null) {
      // MetadataAttribution.actor is non-null; an actor of an entity type GraphQL can't map (e.g.
      // added by a newer version before a rollback) leaves the attribution out.
      return null;
    }
    final MetadataAttribution result = new MetadataAttribution();
    result.setTime(input.getTime());
    result.setActor(actor);
    if (input.getSource() != null) {
      result.setSource(UrnToEntityMapper.map(context, input.getSource()));
    }
    if (input.getSourceDetail() != null) {
      result.setSourceDetail(StringMapMapper.map(context, input.getSourceDetail()));
    }
    return result;
  }
}
