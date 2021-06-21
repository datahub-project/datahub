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
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  void upsertDocument(@Nonnull String document, @Nonnull String docId);

  /**
   * Get a list of buckets that match a set of criteria.
   */
  @Nonnull
  List<UsageAggregation> query(@Nonnull String resource, @Nonnull WindowDuration window, Long startTime, Long endTime, Integer maxBuckets);
}
