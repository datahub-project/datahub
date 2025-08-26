package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.*;

import com.linkedin.actionrequest.ActionRequestStatus;
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
import javax.annotation.Nullable;

public class ActionRequestStatusChangeEventGenerator
    extends EntityChangeEventGenerator<ActionRequestStatus> {

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<ActionRequestStatus> from,
      @Nonnull Aspect<ActionRequestStatus> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final ActionRequestStatus previousAspect,
      final ActionRequestStatus newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {
    if (newAspect == null) {
      return Collections.emptyList();
    }
    if (previousAspect != null && previousAspect.getStatus().equals(newAspect.getStatus())) {
      return Collections.emptyList();
    }

    final ChangeOperation changeOperation = getChangeOperation(newAspect);
    if (changeOperation == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.valueOf(newAspect.getStatus()))
            .auditStamp(auditStamp)
            .entityUrn(entityUrn)
            .parameters(buildParameters(newAspect))
            .build());
  }

  @Nullable
  private ChangeOperation getChangeOperation(
      @Nonnull final ActionRequestStatus actionRequestStatus) {
    if (actionRequestStatus.getStatus().equals(ACTION_REQUEST_STATUS_PENDING)) {
      return ChangeOperation.CREATE;
    }
    if (actionRequestStatus.getStatus().equals(ACTION_REQUEST_STATUS_COMPLETE)) {
      return ChangeOperation.MODIFY;
    }
    return null;
  }

  @Nonnull
  private Map<String, Object> buildParameters(
      @Nonnull final ActionRequestStatus actionRequestStatus) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(ACTION_REQUEST_STATUS_KEY, actionRequestStatus.getStatus());
    if (actionRequestStatus.hasResult()) {
      parameters.put(ACTION_REQUEST_RESULT_KEY, actionRequestStatus.getResult());
    }
    return parameters;
  }
}
