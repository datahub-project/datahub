package com.linkedin.metadata.service;

import com.linkedin.data.template.GetMode;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class IncidentInfoArgumentMatcher implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public IncidentInfoArgumentMatcher(MetadataChangeProposal left) {
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
    IncidentInfo leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", IncidentInfo.class);

    IncidentInfo rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", IncidentInfo.class);

    // Verify optional fields.
    if (leftProps.hasTitle()) {
      if (!leftProps.getTitle().equals(rightProps.getTitle())) {
        return false;
      }
    }

    if (leftProps.hasDescription()) {
      if (!leftProps.getDescription().equals(rightProps.getDescription())) {
        return false;
      }
    }

    if (leftProps.hasPriority()) {
      if (!leftProps.getPriority().equals(rightProps.getPriority())) {
        return false;
      }
    }

    if (leftProps.hasCustomType()) {
      if (!leftProps.getCustomType().equals(rightProps.getCustomType())) {
        return false;
      }
    }

    if (leftProps.hasSource()) {
      if (!leftProps.getSource().equals(rightProps.getSource())) {
        return false;
      }
    }

    // Verify required fields.
    return leftProps.getType().equals(rightProps.getType())
        && leftProps.getEntities(GetMode.NULL).equals(rightProps.getEntities(GetMode.NULL))
        && leftProps.getStatus().getState().equals(rightProps.getStatus().getState())
        && leftProps
            .getStatus()
            .getLastUpdated()
            .getActor()
            .equals(rightProps.getStatus().getLastUpdated().getActor())
        && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor());
  }
}
