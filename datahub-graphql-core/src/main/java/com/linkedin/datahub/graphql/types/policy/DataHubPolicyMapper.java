package com.linkedin.datahub.graphql.types.policy;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.DataHubPolicy;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
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
    // Type/state are free-form strings; while unknown enum values are tolerated, a value this
    // version doesn't know (ingested via the API, or written by a newer version before a rollback)
    // falls back to the same values PolicyInfoPolicyMapper uses. Otherwise it fails as before.
    policy.setPolicyType(
        UnknownEnumValues.fromString(PolicyType.class, policyInfo.getType(), PolicyType.METADATA));
    policy.setState(
        UnknownEnumValues.fromString(
            PolicyState.class, policyInfo.getState(), PolicyState.INACTIVE));
    policy.setName(policyInfo.getDisplayName()); // Rebrand to 'name'
    policy.setPrivileges(policyInfo.getPrivileges());
    policy.setActors(mapActors(policyInfo.getActors()));
    policy.setEditable(policyInfo.isEditable());
    if (policyInfo.hasResources()) {
      policy.setResources(PolicyInfoPolicyMapper.mapResources(context, policyInfo.getResources()));
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
}
