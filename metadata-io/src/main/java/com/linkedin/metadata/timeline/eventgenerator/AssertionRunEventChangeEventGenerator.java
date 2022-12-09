package com.linkedin.metadata.timeline.eventgenerator;

import com.google.common.collect.ImmutableSortedMap;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class AssertionRunEventChangeEventGenerator extends EntityChangeEventGenerator<AssertionRunEvent> {
  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<AssertionRunEvent> from,
      @Nonnull Aspect<AssertionRunEvent> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      @Nonnull final AssertionRunEvent previousAspect,
      @Nonnull final AssertionRunEvent newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {

    boolean isPreviousCompleted = isCompleted(previousAspect);
    boolean isNewCompleted = isCompleted(newAspect);

    if (isNewCompleted && !isPreviousCompleted) {
      return Collections.singletonList(ChangeEvent.builder()
          .category(ChangeCategory.RUN)
          .operation(ChangeOperation.COMPLETED)
          .auditStamp(auditStamp)
          .entityUrn(entityUrn)
          .parameters(buildParameters(newAspect))
          .build());
    }

    return Collections.emptyList();
  }

  private boolean isCompleted(final AssertionRunEvent assertionRunEvent) {
    return assertionRunEvent != null && assertionRunEvent.getStatus()
        .toString()
        .equals(ASSERTION_RUN_EVENT_STATUS_COMPLETE);
  }

  @Nonnull
  private Map<String, Object> buildParameters(@Nonnull final AssertionRunEvent assertionRunEvent) {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(RUN_RESULT_KEY, assertionRunEvent.getStatus().toString());
    parameters.put(RUN_ID_KEY, assertionRunEvent.getRunId());
    parameters.put(ASSERTEE_URN_KEY, assertionRunEvent.getAsserteeUrn().toString());

    if (assertionRunEvent.hasResult()) {
      final AssertionResult assertionResult = assertionRunEvent.getResult();
      parameters.put(ASSERTION_RESULT_KEY, assertionResult.getType().toString());
    }

    return ImmutableSortedMap.copyOf(parameters);
  }
}
