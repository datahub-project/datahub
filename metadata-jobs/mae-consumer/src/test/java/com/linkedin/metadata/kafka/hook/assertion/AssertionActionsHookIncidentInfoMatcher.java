package com.linkedin.metadata.kafka.hook.assertion;

import com.linkedin.data.template.GetMode;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class AssertionActionsHookIncidentInfoMatcher
    implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public AssertionActionsHookIncidentInfoMatcher(MetadataChangeProposal left) {
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

    System.out.println(String.format("COMPARING LEFT %s", leftProps));
    System.out.println(String.format("COMPARING RIGHT %s", leftProps));

    // Omit timestamp comparison.
    return leftProps.getTitle(GetMode.NULL).equals(rightProps.getTitle(GetMode.NULL))
            && leftProps
                .getDescription(GetMode.NULL)
                .equals(rightProps.getDescription(GetMode.NULL))
            && leftProps.getType().equals(rightProps.getType())
            && leftProps.getEntities(GetMode.NULL).equals(rightProps.getEntities(GetMode.NULL))
            && leftProps.getStatus().getState().equals(rightProps.getStatus().getState())
            && leftProps
                .getStatus()
                .getMessage(GetMode.NULL)
                .equals(rightProps.getStatus().getMessage(GetMode.NULL))
            && leftProps
                .getStatus()
                .getLastUpdated()
                .getActor()
                .equals(rightProps.getStatus().getLastUpdated().getActor())
            && leftProps.getSource().getType().equals(rightProps.getSource().getType())
            && leftProps
                .getSource()
                .getSourceUrn(GetMode.NULL)
                .equals(rightProps.getSource().getSourceUrn(GetMode.NULL))
            && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor())
            && leftProps.hasPriority()
        ? leftProps.getPriority().equals(rightProps.getPriority(GetMode.NULL))
        : true;
  }
}
