package com.linkedin.datahub.graphql.types.policy.mappers;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubPolicyInfo;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class PolicyMapper implements ModelMapper<EntityResponse, Policy> {

  public static final PolicyMapper INSTANCE = new PolicyMapper();

  public static Policy map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public Policy apply(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      // If the policy exists, it should always have DataHubPolicyInfo.
      throw new IllegalArgumentException(
          String.format("Failed to find DataHubPolicyInfo aspect in DataHubPolicy data %s. Invalid state.",
              entityResponse));
    }
    DataMap dataMap = aspectMap.get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data();
    Policy result = PolicyInfoPolicyMapper.map(new DataHubPolicyInfo(dataMap));
    result.setUrn(entityResponse.getUrn().toString());
    return result;
  }
}
