package com.linkedin.metadata.timeline.eventgenerator;

import com.google.common.collect.ImmutableSortedMap;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class AssertionRunEventChangeEventGenerator extends EntityChangeEventGenerator<AssertionRunEvent> {
  @Override
  public List<ChangeEvent> getChangeEvents(@Nonnull Urn urn, @Nonnull String entity, @Nonnull String aspect,
      @Nonnull Aspect<AssertionRunEvent> from, @Nonnull Aspect<AssertionRunEvent> to, @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(AssertionRunEvent baseAspect, AssertionRunEvent targetAspect, String entityUrn,
      AuditStamp auditStamp) {

    boolean isBaseCompleted = isCompleted(baseAspect);
    boolean isTargetCompleted = isCompleted(targetAspect);

    if (isTargetCompleted && !isBaseCompleted) {
      Map<String, Object> paramsMap =
          ImmutableSortedMap.of(RUN_RESULT_KEY, ASSERTION_RUN_EVENT_STATUS_COMPLETE, RUN_ID_KEY,
              targetAspect.getRunId(), ASSERTEE_URN_KEY, targetAspect.getAsserteeUrn().toString());

      return Collections.singletonList(ChangeEvent.builder()
          .category(ChangeCategory.RUN)
          .operation(ChangeOperation.COMPLETED)
          .auditStamp(auditStamp)
          .entityUrn(entityUrn)
          .parameters(paramsMap)
          .build());
    }

    return Collections.emptyList();
  }

  private boolean isCompleted(AssertionRunEvent assertionRunEvent) {
    return assertionRunEvent != null && assertionRunEvent.getStatus()
        .toString()
        .equals(ASSERTION_RUN_EVENT_STATUS_COMPLETE);
  }
}
