package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import org.mockito.ArgumentMatcher;

public class CreateSecretResolverMatcherTest implements ArgumentMatcher<MetadataChangeProposal> {

  private MetadataChangeProposal left;

  public CreateSecretResolverMatcherTest(MetadataChangeProposal left) {
    this.left = left;
  }

  @Override
  public boolean matches(MetadataChangeProposal right) {
    return left.getEntityType().equals(right.getEntityType())
        && left.getAspectName().equals(right.getAspectName())
        && left.getChangeType().equals(right.getChangeType())
        && secretPropertiesMatch(left.getAspect(), right.getAspect());
  }

  private boolean secretPropertiesMatch(GenericAspect left, GenericAspect right) {
    DataHubSecretValue leftProps =
        GenericRecordUtils.deserializeAspect(
            left.getValue(), "application/json", DataHubSecretValue.class);

    DataHubSecretValue rightProps =
        GenericRecordUtils.deserializeAspect(
            right.getValue(), "application/json", DataHubSecretValue.class);

    // Omit timestamp comparison.
    return leftProps.getName().equals(rightProps.getName())
        && leftProps.getValue().equals(rightProps.getValue())
        && leftProps.getDescription().equals(rightProps.getDescription())
        && leftProps.getCreated().getActor().equals(rightProps.getCreated().getActor());
  }
}
