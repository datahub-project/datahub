package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;

public class CreateBusinessAttributeProposalMatcher
    implements ArgumentMatcher<MetadataChangeProposal> {
  private MetadataChangeProposal left;

  public CreateBusinessAttributeProposalMatcher(MetadataChangeProposal left) {
    this.left = left;
  }

  @Override
  public boolean matches(MetadataChangeProposal right) {
    return left.getEntityType().equals(right.getEntityType())
        && left.getAspectName().equals(right.getAspectName())
        && left.getChangeType().equals(right.getChangeType())
        && businessAttributeInfoMatch(left.getAspect(), right.getAspect());
  }

  private boolean businessAttributeInfoMatch(GenericAspect left, GenericAspect right) {
    BusinessAttributeInfo leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", BusinessAttributeInfo.class);

    BusinessAttributeInfo rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", BusinessAttributeInfo.class);

    return leftProps.getName().equals(rightProps.getName())
        && leftProps.getDescription().equals(rightProps.getDescription());
  }
}
