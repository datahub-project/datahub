/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
      return com.linkedin.datahub.graphql.generated.DataHubPageModuleType.valueOf(type.toString());
    } catch (IllegalArgumentException e) {
      log.warn("Encountered unexpected DataHub Page Module type", e);
      return com.linkedin.datahub.graphql.generated.DataHubPageModuleType.UNKNOWN;
    }
  }
}
