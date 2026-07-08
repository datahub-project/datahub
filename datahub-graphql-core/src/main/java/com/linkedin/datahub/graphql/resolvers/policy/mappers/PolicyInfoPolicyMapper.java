package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.generated.PolicyMatchCondition;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterion;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionValue;
import com.linkedin.datahub.graphql.generated.PolicyMatchFilter;
import com.linkedin.datahub.graphql.generated.PolicyState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.datahub.graphql.generated.ResourceFilter;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps {@link com.linkedin.policy.DataHubPolicyInfo} to GraphQL {@link
 * com.linkedin.datahub.graphql.generated.Policy}.
 */
@Slf4j
public class PolicyInfoPolicyMapper implements ModelMapper<DataHubPolicyInfo, Policy> {

  public static final PolicyInfoPolicyMapper INSTANCE = new PolicyInfoPolicyMapper();

  public static Policy map(
      @Nullable QueryContext context, @Nonnull final DataHubPolicyInfo policyInfo) {
    return INSTANCE.apply(context, policyInfo);
  }

  @Override
  public Policy apply(@Nullable QueryContext context, DataHubPolicyInfo info) {
    final Policy result = new Policy();
    result.setDescription(info.getDescription());
    // Type/state are free-form strings in the metadata model, so ingested or API-created policies
    // can carry values outside these GraphQL enums. Fall back to a safe default rather than throw,
    // so a single malformed policy does not fail the whole listPolicies request (which otherwise
    // surfaces as "Failed to load policies!" on any page containing it).
    result.setType(safeValueOf(PolicyType.class, info.getType(), PolicyType.METADATA));
    result.setState(safeValueOf(PolicyState.class, info.getState(), PolicyState.INACTIVE));
    result.setName(info.getDisplayName()); // Rebrand to 'name'
    result.setPrivileges(info.getPrivileges());
    result.setActors(mapActors(info.getActors()));
    result.setEditable(info.isEditable());
    if (info.hasResources()) {
      result.setResources(mapResources(context, info.getResources()));
    }
    return result;
  }

  @Nonnull
  private static <T extends Enum<T>> T safeValueOf(
      @Nonnull final Class<T> enumClass, @Nullable final String value, @Nonnull final T fallback) {
    try {
      return Enum.valueOf(enumClass, value);
    } catch (IllegalArgumentException | NullPointerException e) {
      log.warn(
          "Unrecognized {} value '{}' on policy; defaulting to {}",
          enumClass.getSimpleName(),
          value,
          fallback);
      return fallback;
    }
  }

  private ActorFilter mapActors(final DataHubActorFilter actorFilter) {
    final ActorFilter result = new ActorFilter();
    result.setAllGroups(actorFilter.isAllGroups());
    result.setAllUsers(actorFilter.isAllUsers());
    result.setResourceOwners(actorFilter.isResourceOwners());
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

  private static ResourceFilter mapResources(
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

  private static PolicyMatchFilter mapFilter(
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
                                    .map(v -> mapValue(context, v))
                                    .collect(Collectors.toList()))
                            .setCondition(
                                PolicyMatchCondition.valueOf(criterion.getCondition().name()))
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private static PolicyMatchCriterionValue mapValue(
      @Nullable QueryContext context, final String value) {
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
