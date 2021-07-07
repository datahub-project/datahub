package com.linkedin.metadata.usage;

import com.linkedin.common.WindowDuration;
import com.linkedin.usage.UsageAggregation;

import javax.annotation.Nonnull;
import java.util.List;

public interface UsageService {

  void configure();

  /**
   * Write an aggregated usage metric bucket.
   *
   * @param bucket the bucket to upsert
   */
  void upsertDocument(@Nonnull UsageAggregation bucket);

    /**
   * Get a list of buckets that match a set of criteria.
   */
  @Nonnull
  List<UsageAggregation> query(@Nonnull String resource, @Nonnull WindowDuration window, Long startTime, Long endTime, Integer maxBuckets);
}
