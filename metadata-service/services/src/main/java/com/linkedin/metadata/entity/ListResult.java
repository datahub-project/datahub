/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity;

import com.linkedin.metadata.query.ListResultMetadata;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

/**
 * An immutable value class that holds the result of a list operation and other pagination
 * information.
 *
 * @param <T> the result type
 */
@AllArgsConstructor
@Builder
@Value
public class ListResult<T> {

  public static final int INVALID_NEXT_START = -1;

  // A single page of results
  List<T> values;

  // Related result metadata
  ListResultMetadata metadata;

  // Offset from the next page
  int nextStart;

  // Whether there's more results
  boolean hasNext;

  // Total number of hits
  int totalCount;

  // Total number of pages
  int totalPageCount;

  // Size of each page
  int pageSize;
}
