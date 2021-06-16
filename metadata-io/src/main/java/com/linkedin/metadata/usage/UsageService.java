package com.linkedin.metadata.usage;

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

}
