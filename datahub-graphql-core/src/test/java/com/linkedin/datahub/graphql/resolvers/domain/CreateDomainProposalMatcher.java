package com.linkedin.datahub.graphql.resolvers.domain;

import com.linkedin.domain.DomainProperties;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.ArgumentMatcher;


public class CreateDomainProposalMatcher implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public CreateDomainProposalMatcher(MetadataChangeProposal left) {
      this.left = left;
  }

  @Override
  public boolean matches(MetadataChangeProposal right) {
    return left.getEntityType().equals(right.getEntityType())
        && left.getAspectName().equals(right.getAspectName())
        && left.getChangeType().equals(right.getChangeType())
        && domainPropertiesMatch(left.getAspect(), right.getAspect());
  }

  private boolean domainPropertiesMatch(GenericAspect left, GenericAspect right) {
    DomainProperties leftProps = GenericRecordUtils.deserializeAspect(
        left.getValue(),
        "application/json",
        DomainProperties.class
    );

    DomainProperties rightProps = GenericRecordUtils.deserializeAspect(
        right.getValue(),
        "application/json",
        DomainProperties.class
    );

    // Omit timestamp comparison.
    return leftProps.getName().equals(rightProps.getName())
        && leftProps.getDescription().equals(rightProps.getDescription())
        && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor());
  }
}
