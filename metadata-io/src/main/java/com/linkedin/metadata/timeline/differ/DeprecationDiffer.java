package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Differ responsible for determining whether an entity has been soft-deleted or soft-created.
 */
public class DeprecationDiffer implements AspectDiffer<Deprecation> {
  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    // TODO: Migrate away from using getSemanticDiff.
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Deprecation> from,
      @Nonnull Aspect<Deprecation> to,
      @Nonnull AuditStamp auditStamp) {
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
