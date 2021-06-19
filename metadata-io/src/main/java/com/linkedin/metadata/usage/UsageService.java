package com.linkedin.metadata.usage;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.query.SearchResult;

import javax.annotation.Nonnull;


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
  SearchResult query(@Nonnull String resource, @Nonnull WindowDuration window, Long start_time, Long end_time);
}
