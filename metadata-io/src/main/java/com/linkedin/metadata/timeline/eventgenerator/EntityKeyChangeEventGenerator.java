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
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * A general purpose differ which simply determines whether an entity has been created or hard
 * deleted.
 */
public class EntityKeyChangeEventGenerator<K extends RecordTemplate>
    extends EntityChangeEventGenerator<K> {
  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<K> from,
      @Nonnull Aspect<K> to,
      @Nonnull AuditStamp auditStamp) {
    if (from.getValue() == null && to.getValue() != null) {
      // Entity Hard Created
      return Collections.singletonList(buildCreateChangeEvent(urn, auditStamp));
    }
    if (from.getValue() != null && to.getValue() == null) {
      // Entity Hard Deleted
      return Collections.singletonList(buildDeleteChangeEvent(urn, auditStamp));
    }
    return Collections.emptyList();
  }

  private ChangeEvent buildCreateChangeEvent(final Urn urn, final AuditStamp auditStamp) {
    return ChangeEvent.builder()
        .entityUrn(urn.toString())
        .category(ChangeCategory.LIFECYCLE)
        .operation(ChangeOperation.CREATE)
        .auditStamp(auditStamp)
        .build();
  }

  private ChangeEvent buildDeleteChangeEvent(final Urn urn, final AuditStamp auditStamp) {
    return ChangeEvent.builder()
        .entityUrn(urn.toString())
        .category(ChangeCategory.LIFECYCLE)
        .operation(ChangeOperation.HARD_DELETE)
        .auditStamp(auditStamp)
        .build();
  }
}
