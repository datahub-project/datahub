/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.elasticsearch.query.request;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;

public class PITAwareSearchRequest extends SearchRequest {
  private IndicesOptions indicesOptions;

  @Override
  public SearchRequest indicesOptions(IndicesOptions indicesOptions) {
    this.indicesOptions = indicesOptions;
    return this;
  }

  @Override
  public IndicesOptions indicesOptions() {
    return indicesOptions;
  }

  @Override
  public boolean isCcsMinimizeRoundtrips() {
    return false;
  }
}
