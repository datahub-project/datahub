package com.linkedin.datahub.graphql.types.module;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.module.DataHubPageModuleType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PageModuleTypeMapper
    implements ModelMapper<
        DataHubPageModuleType, com.linkedin.datahub.graphql.generated.DataHubPageModuleType> {

  public static final PageModuleTypeMapper INSTANCE = new PageModuleTypeMapper();

  public static com.linkedin.datahub.graphql.generated.DataHubPageModuleType map(
      @Nonnull final DataHubPageModuleType type) {
    return INSTANCE.apply(null, type);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataHubPageModuleType apply(
      @Nullable final QueryContext context, @Nonnull final DataHubPageModuleType type) {
    return com.linkedin.datahub.graphql.generated.DataHubPageModuleType.valueOf(type.toString());
  }
}
