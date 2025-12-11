/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
