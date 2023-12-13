package com.linkedin.metadata.kafka.hook.assertion;

import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class AssertionActionsHookAnomalyInfoMatcher
    implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public AssertionActionsHookAnomalyInfoMatcher(MetadataChangeProposal left) {
    this.left = left;
  }

  @Override
  public boolean matches(MetadataChangeProposal right) {
    return left.getEntityType().equals(right.getEntityType())
        && left.getAspectName().equals(right.getAspectName())
        && left.getChangeType().equals(right.getChangeType())
        && incidentInfoMatches(left.getAspect(), right.getAspect());
  }

  private boolean incidentInfoMatches(GenericAspect left, GenericAspect right) {
    AnomalyInfo leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", AnomalyInfo.class);

    AnomalyInfo rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", AnomalyInfo.class);

    // Verify optional fields.
    if (leftProps.hasDescription()) {
      if (!leftProps.getDescription().equals(rightProps.getDescription())) {
        return false;
      }
    }

    if (leftProps.hasSeverity()) {
      if (!leftProps.getSeverity().equals(rightProps.getSeverity())) {
        return false;
      }
    }

    if (leftProps.getStatus().hasProperties()
        && leftProps.getStatus().getProperties().hasAssertionRunEventTime()) {
      if (!leftProps
          .getStatus()
          .getProperties()
          .getAssertionRunEventTime()
          .equals(rightProps.getStatus().getProperties().getAssertionRunEventTime())) {
        return false;
      }
    }

    if (leftProps.getSource().hasProperties()
        && leftProps.getSource().getProperties().hasAssertionRunEventTime()) {
      if (!leftProps
          .getSource()
          .getProperties()
          .getAssertionRunEventTime()
          .equals(rightProps.getSource().getProperties().getAssertionRunEventTime())) {
        return false;
      }
    }

    if (leftProps.getSource().hasSourceUrn()) {
      if (!leftProps.getSource().getSourceUrn().equals(rightProps.getSource().getSourceUrn())) {
        return false;
      }
    }

    // Verify required fields.
    return leftProps.getType().equals(rightProps.getType())
        && leftProps.getEntity().equals(rightProps.getEntity())
        && leftProps.getStatus().getState().equals(rightProps.getStatus().getState())
        && leftProps
            .getStatus()
            .getLastUpdated()
            .getActor()
            .equals(rightProps.getStatus().getLastUpdated().getActor())
        && leftProps.getSource().getType().equals(rightProps.getSource().getType())
        && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor());
  }
}
