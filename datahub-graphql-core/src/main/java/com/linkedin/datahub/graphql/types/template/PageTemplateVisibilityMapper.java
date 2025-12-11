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
import com.linkedin.datahub.graphql.generated.PageTemplateScope;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.template.DataHubPageTemplateVisibility;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageTemplateVisibilityMapper
    implements ModelMapper<
        DataHubPageTemplateVisibility,
        com.linkedin.datahub.graphql.generated.DataHubPageTemplateVisibility> {

  public static final PageTemplateVisibilityMapper INSTANCE = new PageTemplateVisibilityMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubPageTemplateVisibility map(
      @Nonnull final DataHubPageTemplateVisibility visibility) {
    return INSTANCE.apply(null, visibility);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubPageTemplateVisibility apply(
      @Nullable final QueryContext context,
      @Nonnull final DataHubPageTemplateVisibility visibility) {
    final com.linkedin.datahub.graphql.generated.DataHubPageTemplateVisibility result =
        new com.linkedin.datahub.graphql.generated.DataHubPageTemplateVisibility();

    if (visibility.hasScope()) {
      result.setScope(PageTemplateScope.valueOf(visibility.getScope().toString()));
    }

    return result;
  }
}
