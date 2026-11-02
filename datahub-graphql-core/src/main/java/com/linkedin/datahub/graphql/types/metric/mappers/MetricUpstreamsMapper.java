package com.linkedin.datahub.graphql.types.metric.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MetricUpstreams;
import com.linkedin.datahub.graphql.types.common.mappers.EdgeMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps {@link com.linkedin.metric.MetricUpstreams} Pegasus records to the generated GraphQL {@link
 * MetricUpstreams}.
 */
public class MetricUpstreamsMapper {

  private MetricUpstreamsMapper() {}

  @Nullable
  public static MetricUpstreams map(
      @Nullable QueryContext context, @Nullable com.linkedin.metric.MetricUpstreams pdl) {
    if (pdl == null) {
      return null;
    }

    MetricUpstreams result = new MetricUpstreams();

    if (pdl.hasDatasetUpstreams() && pdl.getDatasetUpstreams() != null) {
      result.setDatasetUpstreams(EdgeMapper.mapList(context, pdl.getDatasetUpstreams()));
    }

    if (pdl.hasFieldUpstreams() && pdl.getFieldUpstreams() != null) {
      result.setFieldUpstreams(EdgeMapper.mapList(context, pdl.getFieldUpstreams()));
    }

    return result;
  }

  @Nonnull
  public static MetricUpstreams mapNonNull(
      @Nonnull QueryContext context, @Nonnull com.linkedin.metric.MetricUpstreams pdl) {
    MetricUpstreams result = map(context, pdl);
    return result != null ? result : new MetricUpstreams();
  }
}
