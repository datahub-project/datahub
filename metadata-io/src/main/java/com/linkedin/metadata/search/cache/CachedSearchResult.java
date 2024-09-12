package com.linkedin.metadata.search.cache;

import static com.datahub.util.RecordUtils.*;
import static com.linkedin.metadata.search.utils.GZIPUtil.*;

import com.linkedin.metadata.search.SearchResult;
import java.io.Serializable;
import lombok.Data;

@Data
public class CachedSearchResult implements Serializable {
  private final byte[] searchResult;
  private final long timestamp;

  public CachedSearchResult(SearchResult lineageResult, long timestamp) {
    this.searchResult = gzipCompress(toJsonString(lineageResult));
    this.timestamp = timestamp;
  }

  public SearchResult getSearchResult() {
    return toRecordTemplate(SearchResult.class, gzipDecompress(searchResult));
  }
}
