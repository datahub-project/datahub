package com.linkedin.datahub.graphql.types.policy;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.DataHubPolicy;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.PolicyMatchCondition;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterion;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionValue;
import com.linkedin.datahub.graphql.generated.PolicyMatchFilter;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.ResourceFilter;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataHubPolicyMapper implements ModelMapper<EntityResponse, DataHubPolicy> {

  public static final DataHubPolicyMapper INSTANCE = new DataHubPolicyMapper();

  public static DataHubPolicy map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataHubPolicy apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataHubPolicy result = new DataHubPolicy();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATAHUB_POLICY);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataHubPolicy> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(context, DATAHUB_POLICY_INFO_ASPECT_NAME, this::mapDataHubPolicyInfo);
    return mappingHelper.getResult();
  }

  private void mapDataHubPolicyInfo(
      @Nullable QueryContext context, @Nonnull DataHubPolicy policy, @Nonnull DataMap dataMap) {
    DataHubPolicyInfo policyInfo = new DataHubPolicyInfo(dataMap);
    policy.setDescription(policyInfo.getDescription());
    // Careful - we assume no other Policy types or states have been ingested using a backdoor.
    policy.setPolicyType(PolicyType.valueOf(policyInfo.getType()));
    policy.setState(PolicyState.valueOf(policyInfo.getState()));
    policy.setName(policyInfo.getDisplayName()); // Rebrand to 'name'
    policy.setPrivileges(policyInfo.getPrivileges());
    policy.setActors(mapActors(policyInfo.getActors()));
    policy.setEditable(policyInfo.isEditable());
    if (policyInfo.hasResources()) {
      policy.setResources(mapResources(context, policyInfo.getResources()));
    }
  }

  private ActorFilter mapActors(final DataHubActorFilter actorFilter) {
    final ActorFilter result = new ActorFilter();
    result.setAllGroups(actorFilter.isAllGroups());
    result.setAllUsers(actorFilter.isAllUsers());
    result.setResourceOwners(actorFilter.isResourceOwners());
    // Change here is not executed at the moment - leaving it for the future
    UrnArray resourceOwnersTypes = actorFilter.getResourceOwnersTypes();
    if (resourceOwnersTypes != null) {
      result.setResourceOwnersTypes(
          resourceOwnersTypes.stream().map(Urn::toString).collect(Collectors.toList()));
    }
    if (actorFilter.hasGroups()) {
      result.setGroups(
          actorFilter.getGroups().stream().map(Urn::toString).collect(Collectors.toList()));
    }
    if (actorFilter.hasUsers()) {
      result.setUsers(
          actorFilter.getUsers().stream().map(Urn::toString).collect(Collectors.toList()));
    }
    if (actorFilter.hasRoles()) {
      result.setRoles(
          actorFilter.getRoles().stream().map(Urn::toString).collect(Collectors.toList()));
    }
    return result;
  }

  private ResourceFilter mapResources(
      @Nullable QueryContext context, final DataHubResourceFilter resourceFilter) {
    final ResourceFilter result = new ResourceFilter();
    result.setAllResources(resourceFilter.isAllResources());
    if (resourceFilter.hasType()) {
      result.setType(resourceFilter.getType());
    }
    if (resourceFilter.hasResources()) {
      result.setResources(resourceFilter.getResources());
    }
    if (resourceFilter.hasFilter()) {
      result.setFilter(mapFilter(context, resourceFilter.getFilter()));
    }
    return result;
  }

  private PolicyMatchFilter mapFilter(
      @Nullable QueryContext context, final com.linkedin.policy.PolicyMatchFilter filter) {
    return PolicyMatchFilter.builder()
        .setCriteria(
            filter.getCriteria().stream()
                .map(
                    criterion ->
                        PolicyMatchCriterion.builder()
                            .setField(criterion.getField())
                            .setValues(
                                criterion.getValues().stream()
                                    .map(c -> mapValue(context, c))
                                    .collect(Collectors.toList()))
                            .setCondition(
                                PolicyMatchCondition.valueOf(criterion.getCondition().name()))
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private PolicyMatchCriterionValue mapValue(@Nullable QueryContext context, final String value) {
    try {
      // If value is urn, set entity field
      Urn urn = Urn.createFromString(value);
      return PolicyMatchCriterionValue.builder()
          .setValue(value)
          .setEntity(UrnToEntityMapper.map(context, urn))
          .build();
    } catch (URISyntaxException e) {
      // Value is not an urn. Just set value
      return PolicyMatchCriterionValue.builder().setValue(value).build();
    }
  }
}
