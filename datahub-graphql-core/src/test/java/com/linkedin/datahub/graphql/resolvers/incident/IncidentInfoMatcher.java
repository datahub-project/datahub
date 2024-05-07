package com.linkedin.datahub.graphql.resolvers.incident;

import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class IncidentInfoMatcher implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public IncidentInfoMatcher(MetadataChangeProposal left) {
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
    if (leftProps.hasDescription()) {
      if (!leftProps.getDescription().equals(rightProps.getDescription())) {
        return false;
      }
    }

    if (leftProps.hasTitle()) {
      if (!leftProps.getTitle().equals(rightProps.getTitle())) {
        return false;
      }
    }

    if (leftProps.hasType()) {
      if (!leftProps.getType().equals(rightProps.getType())) {
        return false;
      }
    }

    if (leftProps.hasAssignees() && leftProps.getAssignees().size() > 0) {
      if (!((Integer) leftProps.getAssignees().size()).equals(rightProps.getAssignees().size())) {
        return false;
      }
      // Just verify the first mapping
      if (!leftProps
          .getAssignees()
          .get(0)
          .getActor()
          .equals(rightProps.getAssignees().get(0).getActor())) {
        return false;
      }
    }

    if (leftProps.hasPriority()) {
      if (!leftProps.getPriority().equals(rightProps.getPriority())) {
        return false;
      }
    }

    if (leftProps.hasEntities()) {
      if (!leftProps.getEntities().equals(rightProps.getEntities())) {
        return false;
      }
    }

    if (leftProps.getStatus().hasStage()) {
      if (!leftProps.getStatus().getStage().equals(rightProps.getStatus().getStage())) {
        return false;
      }
    }

    if (leftProps.getStatus().hasState()) {
      if (!leftProps.getStatus().getState().equals(rightProps.getStatus().getState())) {
        return false;
      }
    }

    if (leftProps.getStatus().hasMessage()) {
      if (!leftProps.getStatus().getMessage().equals(rightProps.getStatus().getMessage())) {
        return false;
      }
    }

    if (leftProps.getSource().hasType()) {
      if (!leftProps.getSource().getType().equals(rightProps.getSource().getType())) {
        return false;
      }
    }

    if (leftProps.getSource().hasSourceUrn()) {
      if (!leftProps.getSource().getSourceUrn().equals(rightProps.getSource().getSourceUrn())) {
        return false;
      }
    }

    // All fields match!
    return true;
  }
}
