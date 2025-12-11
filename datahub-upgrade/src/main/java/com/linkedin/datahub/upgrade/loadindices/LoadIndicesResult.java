/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.loadindices;

import lombok.Data;

@Data
public class LoadIndicesResult {
  public int rowsProcessed = 0;
  public int ignored = 0;
  public long timeSqlQueryMs = 0;
  public long timeElasticsearchWriteMs = 0;

  @Override
  public String toString() {
    return String.format(
        "LoadIndicesResult{rowsProcessed=%d, ignored=%d, timeSqlQueryMs=%d, timeElasticsearchWriteMs=%d}",
        rowsProcessed, ignored, timeSqlQueryMs, timeElasticsearchWriteMs);
  }
}
