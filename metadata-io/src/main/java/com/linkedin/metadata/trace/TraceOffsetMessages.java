package com.linkedin.metadata.trace;

/** Status text for a consumer-offset comparison. Shared so the search grace matches the readers. */
final class TraceOffsetMessages {
  static final String NOT_PROCESSED = "Consumer has not processed offset.";
  static final String PROCESSED_PAST = "Consumer has processed past the offset.";
  static final String SEARCH_VISIBILITY_GRACE =
      "Consumer has processed past the offset; waiting for search visibility.";

  private TraceOffsetMessages() {}
}
