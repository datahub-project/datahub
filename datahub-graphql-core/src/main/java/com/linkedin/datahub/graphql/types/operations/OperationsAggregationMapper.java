/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.operations;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OperationsAggregation;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OperationsAggregationMapper
    implements ModelMapper<com.linkedin.operations.OperationsAggregation, OperationsAggregation> {

  public static final OperationsAggregationMapper INSTANCE = new OperationsAggregationMapper();

  public static OperationsAggregation map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsAggregation pdlUsageAggregation) {
    return INSTANCE.apply(context, pdlUsageAggregation);
  }

  @Override
  public OperationsAggregation apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsAggregation pdlUsageAggregation) {
    OperationsAggregation result = new OperationsAggregation();
    result.setBucket(pdlUsageAggregation.getBucket());

    if (pdlUsageAggregation.hasDuration()) {
      result.setDuration(WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString()));
    }
    if (pdlUsageAggregation.hasResource()) {
      result.setResource(pdlUsageAggregation.getResource().toString());
    }
    if (pdlUsageAggregation.hasAggregations()) {
      result.setAggregations(
          OperationsAggregationsResultMapper.map(context, pdlUsageAggregation.getAggregations()));
    }
    return result;
  }
}
