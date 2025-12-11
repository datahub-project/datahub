/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UsageAggregation;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UsageAggregationMapper
    implements ModelMapper<com.linkedin.usage.UsageAggregation, UsageAggregation> {

  public static final UsageAggregationMapper INSTANCE = new UsageAggregationMapper();

  public static UsageAggregation map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UsageAggregation pdlUsageAggregation) {
    return INSTANCE.apply(context, pdlUsageAggregation);
  }

  @Override
  public UsageAggregation apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UsageAggregation pdlUsageAggregation) {
    UsageAggregation result = new UsageAggregation();
    result.setBucket(pdlUsageAggregation.getBucket());

    if (pdlUsageAggregation.hasDuration()) {
      result.setDuration(WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString()));
    }
    if (pdlUsageAggregation.hasResource()) {
      result.setResource(pdlUsageAggregation.getResource().toString());
    }
    if (pdlUsageAggregation.hasMetrics()) {
      result.setMetrics(
          UsageAggregationMetricsMapper.map(context, pdlUsageAggregation.getMetrics()));
    }
    return result;
  }
}
