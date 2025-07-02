package com.linkedin.datahub.graphql.types.module;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PageModuleScope;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.module.DataHubPageModuleVisibility;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageModuleVisibilityMapper
    implements ModelMapper<
        DataHubPageModuleVisibility,
        com.linkedin.datahub.graphql.generated.DataHubPageModuleVisibility> {

  public static final PageModuleVisibilityMapper INSTANCE = new PageModuleVisibilityMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubPageModuleVisibility map(
      @Nonnull final DataHubPageModuleVisibility visibility) {
    return INSTANCE.apply(null, visibility);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubPageModuleVisibility apply(
      @Nullable final QueryContext context, @Nonnull final DataHubPageModuleVisibility visibility) {
    final com.linkedin.datahub.graphql.generated.DataHubPageModuleVisibility result =
        new com.linkedin.datahub.graphql.generated.DataHubPageModuleVisibility();

    if (visibility.hasScope()) {
      result.setScope(PageModuleScope.valueOf(visibility.getScope().toString()));
    }

    return result;
  }
}
