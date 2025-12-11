/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity.retention;

import lombok.Data;

@Data
public class BulkApplyRetentionResult {
  public long argStart;
  public long argCount;
  public long argAttemptWithVersion;
  public String argUrn;
  public String argAspectName;
  public long rowsHandled = 0;
  public long timeRetentionPolicyMapMs;
  public long timeRowMs;
  public long timeApplyRetentionMs = 0;
}
