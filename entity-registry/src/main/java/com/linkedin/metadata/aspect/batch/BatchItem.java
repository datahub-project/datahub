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

  /**
   * Determines if this item is a duplicate of another item in terms of the operation it represents
   * to the database.Each implementation can define what constitutes a duplicate based on its
   * specific fields which are persisted.
   */
  boolean isDatabaseDuplicateOf(BatchItem other);
}
