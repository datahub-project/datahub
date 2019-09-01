package com.linkedin.metadata.dao;

import com.linkedin.metadata.query.ListResultMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Value;


/**
 * An immutable value class that holds the result of a list operation and other pagination information.
 *
 * @param <T> the result type
 */
@Builder
@Value
public class ListResult<T> {

  public static final int INVALID_NEXT_START = -1;

  // A single page of results
  List<T> values;

  // Related search result metadata
  ListResultMetadata metadata;

  // Offset from the next page
  int nextStart;

  // Whether there's more results
  boolean havingMore;

  // Total number of hits
  int totalCount;

  // Total number of pages
  int totalPageCount;

  // Size of each page
  int pageSize;
}
