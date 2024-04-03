package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActorFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyMatchFilterInput;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.generated.ResourceFilterInput;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps GraphQL {@link PolicyUpdateInput} to DataHub backend {@link DataHubPolicyInfo}. */
public class PolicyUpdateInputInfoMapper
    implements ModelMapper<PolicyUpdateInput, DataHubPolicyInfo> {

  public static final PolicyUpdateInputInfoMapper INSTANCE = new PolicyUpdateInputInfoMapper();

  public static DataHubPolicyInfo map(
      @Nullable QueryContext context, @Nonnull final PolicyUpdateInput policyInput) {
    return INSTANCE.apply(context, policyInput);
  }

  @Override
  public DataHubPolicyInfo apply(
      @Nullable QueryContext queryContext, @Nonnull final PolicyUpdateInput policyInput) {
    final DataHubPolicyInfo result = new DataHubPolicyInfo();
    result.setDescription(policyInput.getDescription());
    result.setType(policyInput.getType().toString());
    result.setDisplayName(policyInput.getName());
    result.setPrivileges(new StringArray(policyInput.getPrivileges()));
    result.setActors(mapActors(policyInput.getActors()));
    result.setState(policyInput.getState().toString());
    if (policyInput.getResources() != null) {
      result.setResources(mapResources(policyInput.getResources()));
    }
    return result;
  }

  private DataHubActorFilter mapActors(final ActorFilterInput actorInput) {
    final DataHubActorFilter result = new DataHubActorFilter();
    result.setAllGroups(actorInput.getAllGroups());
    result.setAllUsers(actorInput.getAllUsers());
    result.setResourceOwners(actorInput.getResourceOwners());
    if (actorInput.getResourceOwnersTypes() != null) {
      result.setResourceOwnersTypes(
          new UrnArray(
              actorInput.getResourceOwnersTypes().stream()
                  .map(this::createUrn)
                  .collect(Collectors.toList())));
    }
    if (actorInput.getGroups() != null) {
      result.setGroups(
          new UrnArray(
              actorInput.getGroups().stream().map(this::createUrn).collect(Collectors.toList())));
    }
    if (actorInput.getUsers() != null) {
      result.setUsers(
          new UrnArray(
              actorInput.getUsers().stream().map(this::createUrn).collect(Collectors.toList())));
    }
    return result;
  }

  private DataHubResourceFilter mapResources(final ResourceFilterInput resourceInput) {
    final DataHubResourceFilter result = new DataHubResourceFilter();
    if (resourceInput.getAllResources() != null) {
      result.setAllResources(resourceInput.getAllResources());
    }
    // This is an implicit mapping between GQL EntityType and Entity Name as known by GMS.
    // Be careful about maintaining this contract.
    if (resourceInput.getType() != null) {
      result.setType(resourceInput.getType());
    }
    if (resourceInput.getResources() != null) {
      result.setResources(new StringArray(resourceInput.getResources()));
    }
    if (resourceInput.getFilter() != null) {
      result.setFilter(mapFilter(resourceInput.getFilter()));
    }
    return result;
  }

  private PolicyMatchFilter mapFilter(final PolicyMatchFilterInput filter) {
    return new PolicyMatchFilter()
        .setCriteria(
            new PolicyMatchCriterionArray(
                filter.getCriteria().stream()
                    .map(
                        criterion ->
                            new PolicyMatchCriterion()
                                .setField(criterion.getField())
                                .setValues(new StringArray(criterion.getValues()))
                                .setCondition(
                                    PolicyMatchCondition.valueOf(criterion.getCondition().name())))
                    .collect(Collectors.toList())));
  }

  private Urn createUrn(String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to convert urnStr %s into an URN object", urnStr), e);
    }
  }
}
