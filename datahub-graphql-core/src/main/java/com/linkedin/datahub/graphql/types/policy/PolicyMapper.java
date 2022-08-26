package com.linkedin.datahub.graphql.types.policy;

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

  public static final com.linkedin.datahub.graphql.types.policy.PolicyMapper INSTANCE =
      new com.linkedin.datahub.graphql.types.policy.PolicyMapper();

  public static Policy map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public Policy apply(@Nonnull final EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      throw new RuntimeException("Cannot map entity response to policy without a DataHubPolicyInfo aspect.");
    }

    DataMap dataMap = aspectMap.get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data();
    DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo(dataMap);
    Policy result = PolicyInfoPolicyMapper.map(dataHubPolicyInfo);
    result.setUrn(entityResponse.getUrn().toString());
    return result;
  }
}
