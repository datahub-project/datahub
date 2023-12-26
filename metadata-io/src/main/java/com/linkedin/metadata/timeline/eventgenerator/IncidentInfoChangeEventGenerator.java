package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

public class IncidentInfoChangeEventGenerator extends EntityChangeEventGenerator<IncidentInfo>{
  @Override
  public List<ChangeEvent> getChangeEvents(@Nonnull Urn urn, @Nonnull String entity, @Nonnull String aspect,
      @Nonnull Aspect<IncidentInfo> from, @Nonnull Aspect<IncidentInfo> to, @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final IncidentInfo previousAspect,
      final IncidentInfo newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {

    if (incidentCreated(previousAspect, newAspect)) {
      return Collections.singletonList(ChangeEvent.builder()
          .category(ChangeCategory.INCIDENT)
          .operation(ChangeOperation.ACTIVE)
          .auditStamp(auditStamp)
          .entityUrn(entityUrn)
          .build());
    }
    
    if (incidentUpdated(previousAspect, newAspect)) {
      ChangeEvent.ChangeEventBuilder changeEventBuilder = ChangeEvent.builder()
          .category(ChangeCategory.INCIDENT)
          .auditStamp(auditStamp)
          .entityUrn(entityUrn);

      // Change was in status
      if(previousAspect.getStatus().getState() != newAspect.getStatus().getState()) {
        if (newAspect.getStatus().getState().equals(IncidentState.RESOLVED)) {
          changeEventBuilder.operation(ChangeOperation.RESOLVED);
        }
        if (newAspect.getStatus().getState().equals(IncidentState.ACTIVE)) {
          changeEventBuilder.operation(ChangeOperation.ACTIVE);
        }
        return Collections.singletonList(changeEventBuilder.build());
      }
    }

    return Collections.emptyList();
  }

  private static boolean incidentCreated(IncidentInfo previousAspect, IncidentInfo newAspect) {
    return previousAspect == null && newAspect != null;
  }

  private static boolean incidentUpdated(IncidentInfo previousAspect, IncidentInfo newAspect) {
    return previousAspect != null && newAspect != null && !previousAspect.equals(newAspect);
  }

}
