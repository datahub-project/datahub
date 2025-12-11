/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.template;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PageTemplateSurfaceType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.template.DataHubPageTemplateSurface;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageTemplateSurfaceMapper
    implements ModelMapper<
        DataHubPageTemplateSurface,
        com.linkedin.datahub.graphql.generated.DataHubPageTemplateSurface> {

  public static final PageTemplateSurfaceMapper INSTANCE = new PageTemplateSurfaceMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubPageTemplateSurface map(
      @Nonnull final DataHubPageTemplateSurface surface) {
    return INSTANCE.apply(null, surface);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubPageTemplateSurface apply(
      @Nullable final QueryContext context, @Nonnull final DataHubPageTemplateSurface surface) {
    final com.linkedin.datahub.graphql.generated.DataHubPageTemplateSurface result =
        new com.linkedin.datahub.graphql.generated.DataHubPageTemplateSurface();

    if (surface.hasSurfaceType()) {
      result.setSurfaceType(PageTemplateSurfaceType.valueOf(surface.getSurfaceType().toString()));
    }

    return result;
  }
}
