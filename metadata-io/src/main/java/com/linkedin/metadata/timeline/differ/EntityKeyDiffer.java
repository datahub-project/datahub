package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * A general purpose differ which simply determines whether an entity has been created or hard deleted.
 */
public class EntityKeyDiffer<K extends RecordTemplate> implements AspectDiffer<K> {

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    // TODO: Migrate callers to use getChangeEvents.
    throw new UnsupportedOperationException();
  }

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
