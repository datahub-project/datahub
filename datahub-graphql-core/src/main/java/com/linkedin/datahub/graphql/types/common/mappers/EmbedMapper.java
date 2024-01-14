package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Embed;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class EmbedMapper implements ModelMapper<com.linkedin.common.Embed, Embed> {

  public static final EmbedMapper INSTANCE = new EmbedMapper();

  public static Embed map(@Nonnull final com.linkedin.common.Embed metadata) {
    return INSTANCE.apply(metadata);
  }

  @Override
  public Embed apply(@Nonnull final com.linkedin.common.Embed input) {
    final Embed result = new Embed();
    result.setRenderUrl(input.getRenderUrl());
    return result;
  }
}
