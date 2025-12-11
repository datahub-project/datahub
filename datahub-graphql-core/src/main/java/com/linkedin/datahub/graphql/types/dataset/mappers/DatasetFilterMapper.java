/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DatasetFilter;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatasetFilterMapper
    implements ModelMapper<com.linkedin.dataset.DatasetFilter, DatasetFilter> {

  public static final DatasetFilterMapper INSTANCE = new DatasetFilterMapper();

  public static DatasetFilter map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.dataset.DatasetFilter metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public DatasetFilter apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.dataset.DatasetFilter input) {
    final DatasetFilter result = new DatasetFilter();
    result.setType(DatasetFilterType.valueOf(input.getType().name()));
    result.setSql(input.getSql());
    return result;
  }
}
