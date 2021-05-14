package com.linkedin.metadata.search.query;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.SearchResultMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Value;


@Builder
@Value
public class BrowseResult {
  // A single page of results
  List<Urn> resultList;

  // Related search result metadata
  SearchResultMetadata searchResultMetadata;

  // Offset from the first result you want to fetch
  int from;

  // Size of each page
  int pageSize;

  // Whether there's more results
  boolean havingMore;

  // Total number of hits
  int totalCount;

  // Total number of pages
  int totalPageCount;
}
