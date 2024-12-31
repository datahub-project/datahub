package com.linkedin.datahub.graphql.types.dataplatform.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatformInfo;
import com.linkedin.datahub.graphql.generated.PlatformType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Deprecated
public class DataPlatformInfoMapper
    implements ModelMapper<com.linkedin.dataplatform.DataPlatformInfo, DataPlatformInfo> {

  public static final DataPlatformInfoMapper INSTANCE = new DataPlatformInfoMapper();

  public static DataPlatformInfo map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.dataplatform.DataPlatformInfo platform) {
    return INSTANCE.apply(context, platform);
  }

  @Override
  public DataPlatformInfo apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.dataplatform.DataPlatformInfo input) {
    final DataPlatformInfo result = new DataPlatformInfo();
    result.setType(PlatformType.valueOf(input.getType().toString()));
    result.setDatasetNameDelimiter(input.getDatasetNameDelimiter());
    if (input.hasDisplayName()) {
      result.setDisplayName(input.getDisplayName());
    }
    if (input.hasLogoUrl()) {
      result.setLogoUrl(input.getLogoUrl().toString());
    }
    return result;
  }
}
