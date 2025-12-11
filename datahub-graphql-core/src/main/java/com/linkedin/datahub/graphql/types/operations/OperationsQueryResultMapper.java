/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.operations;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OperationsAggregationsResult;
import com.linkedin.datahub.graphql.generated.OperationsQueryResult;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OperationsQueryResultMapper
    implements ModelMapper<com.linkedin.operations.OperationsQueryResult, OperationsQueryResult> {

  public static final OperationsQueryResult EMPTY =
      new OperationsQueryResult(ImmutableList.of(), new OperationsAggregationsResult());

  public static final OperationsQueryResultMapper INSTANCE = new OperationsQueryResultMapper();

  public static OperationsQueryResult map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsQueryResult pdlUsageResult) {
    return INSTANCE.apply(context, pdlUsageResult);
  }

  @Override
  public OperationsQueryResult apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsQueryResult pdlUsageResult) {
    OperationsQueryResult result = new OperationsQueryResult();
    if (pdlUsageResult.hasAggregations()) {
      result.setAggregations(
          OperationsAggregationsResultMapper.map(context, pdlUsageResult.getAggregations()));
    }
    if (pdlUsageResult.hasBuckets()) {
      result.setBuckets(
          pdlUsageResult.getBuckets().stream()
              .map(bucket -> OperationsAggregationMapper.map(context, bucket))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
