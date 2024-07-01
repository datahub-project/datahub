package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LineageFeatures;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class LineageFeaturesMapper
    implements ModelMapper<com.linkedin.metadata.search.features.LineageFeatures, LineageFeatures> {

  public static final LineageFeaturesMapper INSTANCE = new LineageFeaturesMapper();

  public static LineageFeatures map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.metadata.search.features.LineageFeatures lineageFeatures) {
    return INSTANCE.apply(context, lineageFeatures);
  }

  public LineageFeatures apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.metadata.search.features.LineageFeatures lineageFeatures) {
    final LineageFeatures result = new LineageFeatures();
    result.setUpstreamCount(lineageFeatures.getUpstreamCount());
    result.setDownstreamCount(lineageFeatures.getDownstreamCount());
    result.setComputedAt(AuditStampMapper.map(context, lineageFeatures.getComputedAt()));
    return result;
  }
}
