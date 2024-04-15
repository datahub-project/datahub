package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SourceCodeUrl;
import com.linkedin.datahub.graphql.generated.SourceCodeUrlType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SourceCodeUrlMapper
    implements ModelMapper<com.linkedin.ml.metadata.SourceCodeUrl, SourceCodeUrl> {
  public static final SourceCodeUrlMapper INSTANCE = new SourceCodeUrlMapper();

  public static SourceCodeUrl map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.ml.metadata.SourceCodeUrl input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public SourceCodeUrl apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.ml.metadata.SourceCodeUrl input) {
    final SourceCodeUrl results = new SourceCodeUrl();
    results.setType(SourceCodeUrlType.valueOf(input.getType().toString()));
    results.setSourceCodeUrl(input.getSourceCodeUrl().toString());
    return results;
  }
}
