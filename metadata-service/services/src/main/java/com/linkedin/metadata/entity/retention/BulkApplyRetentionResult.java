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
