/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Differ responsible for determining whether an entity has been soft-deleted or soft-created. */
public class StatusChangeEventGenerator extends EntityChangeEventGenerator<Status> {
  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Status> from,
      @Nonnull Aspect<Status> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      Status baseStatus, Status targetStatus, String entityUrn, AuditStamp auditStamp) {

    // If the new status is "removed", then return a soft-deletion event.
    if (isRemoved(targetStatus)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.LIFECYCLE)
              .operation(ChangeOperation.SOFT_DELETE)
              .auditStamp(auditStamp)
              .entityUrn(entityUrn)
              .build());
    }

    // If the new status is "unremoved", then return an reinstatement event.
    if (!isRemoved(targetStatus)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.LIFECYCLE)
              .operation(ChangeOperation.REINSTATE)
              .auditStamp(auditStamp)
              .entityUrn(entityUrn)
              .build());
    }

    return Collections.emptyList();
  }

  private boolean isRemoved(@Nullable final Status status) {
    return status != null && status.isRemoved();
  }
}
