package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Embed;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EmbedMapper implements ModelMapper<com.linkedin.common.Embed, Embed> {

  public static final EmbedMapper INSTANCE = new EmbedMapper();

  public static Embed map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Embed metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public Embed apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Embed input) {
    final Embed result = new Embed();
    result.setRenderUrl(input.getRenderUrl());
    return result;
  }
}
