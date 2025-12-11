/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
