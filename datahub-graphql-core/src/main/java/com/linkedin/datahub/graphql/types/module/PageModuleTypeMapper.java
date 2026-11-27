package com.linkedin.datahub.graphql.types.module;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.module.DataHubPageModuleType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
    try {
      // Already falls back to UNKNOWN whatever the flags; written this way for the lint.
      return Enum.valueOf(
          com.linkedin.datahub.graphql.generated.DataHubPageModuleType.class, type.toString());
    } catch (IllegalArgumentException e) {
      log.warn("Encountered unexpected DataHub Page Module type", e);
      return com.linkedin.datahub.graphql.generated.DataHubPageModuleType.UNKNOWN;
    }
  }
}
