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
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataPlatformInstanceAspectMapper
    implements ModelMapper<com.linkedin.common.DataPlatformInstance, DataPlatformInstance> {

  public static final DataPlatformInstanceAspectMapper INSTANCE =
      new DataPlatformInstanceAspectMapper();

  public static DataPlatformInstance map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.DataPlatformInstance dataPlatformInstance) {
    return INSTANCE.apply(context, dataPlatformInstance);
  }

  @Override
  public DataPlatformInstance apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.DataPlatformInstance input) {
    final DataPlatformInstance result = new DataPlatformInstance();
    if (input.hasInstance()) {
      result.setType(EntityType.DATA_PLATFORM_INSTANCE);
      result.setUrn(input.getInstance().toString());
    }
    // Warning: This often cannot be read properly: overwritten by LoadableTypeResolver
    result.setPlatform(
        DataPlatform.builder()
            .setUrn(input.getPlatform().toString())
            .setType(EntityType.DATA_PLATFORM)
            .build());
    return result;
  }
}
