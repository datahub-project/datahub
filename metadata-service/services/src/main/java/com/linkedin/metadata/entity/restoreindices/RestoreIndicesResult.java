/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity.restoreindices;

import lombok.Data;

@Data
public class RestoreIndicesResult {
  public int ignored = 0;
  public int rowsMigrated = 0;
  public long timeSqlQueryMs = 0;
  public long timeGetRowMs = 0;
  public long timeUrnMs = 0;
  public long timeEntityRegistryCheckMs = 0;
  public long aspectCheckMs = 0;
  public long createRecordMs = 0;
  public long sendMessageMs = 0;
  public long defaultAspectsCreated = 0;
  public String lastUrn = "";
  public String lastAspect = "";
}
