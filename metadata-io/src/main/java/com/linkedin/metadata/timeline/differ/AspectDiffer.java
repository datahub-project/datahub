package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.List;
import javax.annotation.Nonnull;


public interface AspectDiffer<T extends RecordTemplate> {
  @Deprecated
  ChangeTransaction getSemanticDiff(EntityAspect previousValue, EntityAspect currentValue, ChangeCategory element,
      JsonPatch rawDiff, boolean rawDiffsRequested);

  /**
   * TODO: Migrate callers of the above API to below. The recomendation is to move timeline response creation into
   * 2-stage. First stage generate change events, second stage derive semantic meaning + filter those change events.
   *
   * Returns all {@link ChangeEvent}s computed from a raw aspect change.
   *
   * Note that the {@link ChangeEvent} list can contain multiple {@link ChangeCategory} inside of it,
   * it is expected that the caller will filter the set of events as required.
   */
  List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<T> from,
      @Nonnull Aspect<T> to,
      @Nonnull AuditStamp auditStamp);
}
