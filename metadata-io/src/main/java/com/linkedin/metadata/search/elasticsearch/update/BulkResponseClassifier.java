package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Optional policy a bulk listener consults after a response arrives. The listener stays generic:
 * the classifier decides which item results are expected and records whatever accounting that
 * policy owns.
 */
@FunctionalInterface
public interface BulkResponseClassifier<R> {

  @Nonnull
  ExpectedItemRejects classify(@Nonnull R response, @Nullable MetricUtils metricUtils);

  /** Counts a classifier recognized. {@code onlyExpected} is true when every failure was one. */
  record ExpectedItemRejects(long rejected, long skipped, boolean onlyExpected) {}
}
