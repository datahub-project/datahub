/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
