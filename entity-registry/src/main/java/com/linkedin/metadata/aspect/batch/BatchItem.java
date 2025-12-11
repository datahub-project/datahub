/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
