package com.linkedin.datahub.upgrade.system.cron;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class CronArgs implements Cloneable {
  public boolean dryRun;
  public String stepType;

  @Override
  public CronArgs clone() {
    try {
      CronArgs clone = (CronArgs) super.clone();
      // TODO: copy mutable state here, so the clone can't change the internals of the original
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }
}
