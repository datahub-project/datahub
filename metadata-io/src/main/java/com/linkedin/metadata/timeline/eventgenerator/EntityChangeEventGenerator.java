package com.linkedin.metadata.timeline.eventgenerator;

import com.datahub.authentication.Authentication;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.List;
import javax.annotation.Nonnull;

/** An abstract class to generate {@link ChangeEvent}s for a given entity aspect. */
public abstract class EntityChangeEventGenerator<T extends RecordTemplate> {
  // TODO: Add a check for supported aspects
  protected SystemEntityClient _entityClient;
  protected Authentication _authentication;

  public EntityChangeEventGenerator() {}

  public EntityChangeEventGenerator(@Nonnull final SystemEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Deprecated
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    // TODO: Migrate away from using getSemanticDiff.
    throw new UnsupportedOperationException();
  }

  /**
   * TODO: Migrate callers of the above API to below. The recommendation is to move timeline
   * response creation into 2-stage. First stage generate change events, second stage derive
   * semantic meaning + filter those change events.
   *
   * <p>Returns all {@link ChangeEvent}s computed from a raw aspect change.
   *
   * <p>Note that the {@link ChangeEvent} list can contain multiple {@link ChangeCategory} inside of
   * it, it is expected that the caller will filter the set of events as required.
   */
  public abstract List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<T> from,
      @Nonnull Aspect<T> to,
      @Nonnull AuditStamp auditStamp);
}
