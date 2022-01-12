package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.ResourceFilter;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Maps {@link com.linkedin.policy.DataHubPolicyInfo} to GraphQL {@link com.linkedin.datahub.graphql.generated.Policy}.
 */
public class PolicyInfoPolicyMapper implements ModelMapper<DataHubPolicyInfo, Policy>  {

  public static final PolicyInfoPolicyMapper INSTANCE = new PolicyInfoPolicyMapper();

  public static Policy map(@Nonnull final DataHubPolicyInfo policyInfo) {
    return INSTANCE.apply(policyInfo);
  }

  @Override
  public Policy apply(DataHubPolicyInfo info) {
    final Policy result = new Policy();
    result.setDescription(info.getDescription());
    // Careful - we assume no other Policy types or states have been ingested using a backdoor.
    result.setType(PolicyType.valueOf(info.getType()));
    result.setState(PolicyState.valueOf(info.getState()));
    result.setName(info.getDisplayName()); // Rebrand to 'name'
    result.setPrivileges(info.getPrivileges());
    result.setActors(mapActors(info.getActors()));
    result.setEditable(info.isEditable());
    if (info.hasResources()) {
      result.setResources(mapResources(info.getResources()));
    }
    return result;
  }

  private ActorFilter mapActors(final DataHubActorFilter actorFilter) {
    final ActorFilter result = new ActorFilter();
    result.setAllGroups(actorFilter.isAllGroups());
    result.setAllUsers(actorFilter.isAllUsers());
    result.setResourceOwners(actorFilter.isResourceOwners());
    if (actorFilter.hasGroups()) {
      result.setGroups(actorFilter.getGroups()
          .stream()
          .map(Urn::toString)
          .collect(Collectors.toList()));
    }
    if (actorFilter.hasUsers()) {
      result.setUsers(actorFilter.getUsers()
          .stream()
          .map(Urn::toString)
          .collect(Collectors.toList()));
    }
    return result;
  }

  private ResourceFilter mapResources(final DataHubResourceFilter resourceFilter) {
    final ResourceFilter result = new ResourceFilter();
    result.setAllResources(resourceFilter.isAllResources());
    if (resourceFilter.hasType()) {
      result.setType(resourceFilter.getType());
    }
    if (resourceFilter.hasResources()) {
      result.setResources(resourceFilter.getResources());
    }
    return result;
  }

}
