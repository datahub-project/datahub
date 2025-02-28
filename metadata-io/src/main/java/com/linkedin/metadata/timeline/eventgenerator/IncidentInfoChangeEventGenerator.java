package com.linkedin.metadata.timeline.eventgenerator;

import com.google.common.collect.ImmutableSortedMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class IncidentInfoChangeEventGenerator extends EntityChangeEventGenerator<IncidentInfo> {
  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<IncidentInfo> from,
      @Nonnull Aspect<IncidentInfo> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final IncidentInfo previousAspect,
      final IncidentInfo newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {

    if (isIncidentCreated(previousAspect, newAspect)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.INCIDENT)
              .operation(mapIncidentState(newAspect.getStatus().getState()))
              .auditStamp(auditStamp)
              .parameters(buildParameters(newAspect))
              .entityUrn(entityUrn)
              .build());
    }

    if (isIncidentUpdated(previousAspect, newAspect)) {
      ChangeEvent.ChangeEventBuilder changeEventBuilder =
          ChangeEvent.builder()
              .category(ChangeCategory.INCIDENT)
              .auditStamp(auditStamp)
              .entityUrn(entityUrn)
              .parameters(buildParameters(newAspect));

      // Change was in status
      if (isIncidentStatusChanged(previousAspect, newAspect)) {
        changeEventBuilder.operation(mapIncidentState(newAspect.getStatus().getState()));
        return Collections.singletonList(changeEventBuilder.build());
      }
      // TODO: Should changes to incidents be surfaced?
    }

    return Collections.emptyList();
  }

  private ChangeOperation mapIncidentState(IncidentState state) {
    if (state.equals(IncidentState.RESOLVED)) {
      return ChangeOperation.RESOLVED;
    }
    return ChangeOperation.ACTIVE;
  }

  private static boolean isIncidentCreated(IncidentInfo previousAspect, IncidentInfo newAspect) {
    return previousAspect == null && newAspect != null;
  }

  private static boolean isIncidentUpdated(IncidentInfo previousAspect, IncidentInfo newAspect) {
    return previousAspect != null && newAspect != null && !previousAspect.equals(newAspect);
  }

  private static boolean isIncidentStatusChanged(
      IncidentInfo previousAspect, IncidentInfo newAspect) {
    return previousAspect.getStatus().getState() != newAspect.getStatus().getState();
  }

  @Nonnull
  private static Map<String, Object> buildParameters(@Nonnull final IncidentInfo incidentInfo) {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(Constants.ENTITY_REF, incidentInfo.getEntities().toString());
    parameters.put(Constants.INCIDENT_TYPE, incidentInfo.getType().toString());
    if (incidentInfo.hasTitle()) {
      parameters.put(Constants.INCIDENT_TITLE, incidentInfo.getTitle());
    }
    if (incidentInfo.hasDescription()) {
      parameters.put(Constants.INCIDENT_DESCRIPTION, incidentInfo.getDescription());
    }
    if (incidentInfo.getStatus().hasStage()) {
      parameters.put(
          Constants.INCIDENT_STATUS_STAGE, incidentInfo.getStatus().getStage().toString());
    }
    if (incidentInfo.getStatus().hasMessage()) {
      parameters.put(Constants.INCIDENT_STATUS_MESSAGE, incidentInfo.getStatus().getMessage());
    }
    return ImmutableSortedMap.copyOf(parameters);
  }
}
