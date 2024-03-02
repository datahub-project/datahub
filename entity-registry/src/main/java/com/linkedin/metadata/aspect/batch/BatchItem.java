package com.linkedin.metadata.aspect.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface BatchItem extends ReadItem {

  /**
   * Timestamp and actor
   *
   * @return the audit information
   */
  @Nullable
  AuditStamp getAuditStamp();

  /**
   * The type of change
   *
   * @return change type
   */
  @Nonnull
  ChangeType getChangeType();
}
