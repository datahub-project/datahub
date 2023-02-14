package com.linkedin.metadata.timeline.eventgenerator;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Differ responsible for determining whether an entity has been soft-deleted or soft-created.
 */
public class DeprecationChangeEventGenerator extends EntityChangeEventGenerator<Deprecation> {
  @Override
  public List<ChangeEvent> getChangeEvents(@Nonnull Urn urn, @Nonnull String entity, @Nonnull String aspect,
      @Nonnull Aspect<Deprecation> from, @Nonnull Aspect<Deprecation> to, @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      Deprecation baseDeprecation,
      Deprecation targetDeprecation,
      String entityUrn,
      AuditStamp auditStamp) {

    // Ensure that it is the deprecation status which has actually been changed.

    // If the entity was not previously deprecated, but is now deprecated, then return a deprecated event.
    if (!isDeprecated(baseDeprecation) && isDeprecated(targetDeprecation)) {
      return Collections.singletonList(
          ChangeEvent.builder()
            .category(ChangeCategory.DEPRECATION)
            .operation(ChangeOperation.MODIFY)
            .entityUrn(entityUrn)
            .auditStamp(auditStamp)
            .parameters(ImmutableMap.of("status", "DEPRECATED"))
            .build());
    }

    // If the entity was previously deprecated, but is not not deprecated, then return a un-deprecated event.
    if (isDeprecated(baseDeprecation) && !isDeprecated(targetDeprecation)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.DEPRECATION)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .parameters(ImmutableMap.of("status", "ACTIVE"))
              .build());
    }

    return Collections.emptyList();
  }

  private boolean isDeprecated(@Nullable final Deprecation deprecation) {
    return deprecation != null && deprecation.isDeprecated();
  }
}
