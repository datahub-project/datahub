package com.linkedin.datahub.graphql.types.dataplatform.mappers;

import com.linkedin.datahub.graphql.generated.DataPlatformProperties;
import com.linkedin.datahub.graphql.generated.PlatformType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class DataPlatformPropertiesMapper
    implements ModelMapper<com.linkedin.dataplatform.DataPlatformInfo, DataPlatformProperties> {

  public static final DataPlatformPropertiesMapper INSTANCE = new DataPlatformPropertiesMapper();

  public static DataPlatformProperties map(
      @Nonnull final com.linkedin.dataplatform.DataPlatformInfo platform) {
    return INSTANCE.apply(platform);
  }

  @Override
  public DataPlatformProperties apply(
      @Nonnull final com.linkedin.dataplatform.DataPlatformInfo input) {
    final DataPlatformProperties result = new DataPlatformProperties();
    result.setType(PlatformType.valueOf(input.getType().toString()));
    result.setDatasetNameDelimiter(input.getDatasetNameDelimiter());
    if (input.getDisplayName() != null) {
      result.setDisplayName(input.getDisplayName());
    }
    if (input.getLogoUrl() != null) {
      result.setLogoUrl(input.getLogoUrl().toString());
    }
    return result;
  }
}
