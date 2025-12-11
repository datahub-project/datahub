/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
    DomainProperties leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", DomainProperties.class);

    DomainProperties rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", DomainProperties.class);

    // Omit timestamp comparison.
    return leftProps.getName().equals(rightProps.getName())
        && leftProps.getDescription().equals(rightProps.getDescription())
        && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor());
  }
}
