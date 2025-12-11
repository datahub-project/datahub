/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResultType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataprocess.DataProcessInstanceRunResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataProcessInstanceRunResultMapper
    implements ModelMapper<
        DataProcessInstanceRunResult,
        com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult> {

  public static final DataProcessInstanceRunResultMapper INSTANCE =
      new DataProcessInstanceRunResultMapper();

  public static com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult map(
      @Nullable QueryContext context, @Nonnull final DataProcessInstanceRunResult input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult apply(
      @Nullable QueryContext context, @Nonnull final DataProcessInstanceRunResult input) {

    final com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult result =
        new com.linkedin.datahub.graphql.generated.DataProcessInstanceRunResult();

    if (input.hasType()) {
      result.setResultType(DataProcessInstanceRunResultType.valueOf(input.getType().toString()));
    }

    if (input.hasNativeResultType()) {
      result.setNativeResultType(input.getNativeResultType());
    }

    return result;
  }
}
