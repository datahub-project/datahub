package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.query.SearchResultMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Value;


/*
 * Search result wrapper with a list of search documents and related search result metadata
 *
 * @param <T> the document type
 */
@Builder
@Value
public class SearchResult<T extends RecordTemplate> {

  // A single page of results
  List<T> documentList;

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
