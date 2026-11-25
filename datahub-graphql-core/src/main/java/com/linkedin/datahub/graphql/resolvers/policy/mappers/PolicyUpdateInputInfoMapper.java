package com.linkedin.datahub.graphql.resolvers.policy.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
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
import com.linkedin.policy.StructuredPropertyCriterionValue;
import com.linkedin.policy.StructuredPropertyCriterionValueArray;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps GraphQL {@link PolicyUpdateInput} onto a backend {@link DataHubPolicyInfo}.
 *
 * <p>Only fields represented in {@link PolicyUpdateInput} / {@link ActorFilterInput} are ever
 * touched. For an update, callers should go through {@link #mapToExisting} with the currently
 * persisted aspect so this mapper mutates it in place instead of building a new one from scratch -
 * that way any field the GraphQL input schema doesn't model (e.g. {@code actors.roles}, the
 * top-level {@code editable} flag, or anything added to the aspect later) survives the edit
 * untouched rather than being silently reset to its default.
 */
public class PolicyUpdateInputInfoMapper
    implements ModelMapper<PolicyUpdateInput, DataHubPolicyInfo> {

  public static final PolicyUpdateInputInfoMapper INSTANCE = new PolicyUpdateInputInfoMapper();

  /** Maps a create input into a brand new {@link DataHubPolicyInfo}. */
  public static DataHubPolicyInfo map(
      @Nullable QueryContext context, @Nonnull final PolicyUpdateInput policyInput) {
    return INSTANCE.apply(context, policyInput);
  }

  /**
   * Applies an update input onto {@code existingInfo}, mutating and returning it in place so fields
   * absent from {@link PolicyUpdateInput} are preserved.
   */
  public static DataHubPolicyInfo mapToExisting(
      @Nullable QueryContext context,
      @Nonnull final PolicyUpdateInput policyInput,
      @Nonnull final DataHubPolicyInfo existingInfo) {
    return INSTANCE.applyToExisting(context, policyInput, existingInfo);
  }

  @Override
  public DataHubPolicyInfo apply(
      @Nullable QueryContext queryContext, @Nonnull final PolicyUpdateInput policyInput) {
    return applyToExisting(queryContext, policyInput, new DataHubPolicyInfo());
  }

  private DataHubPolicyInfo applyToExisting(
      @Nullable QueryContext queryContext,
      @Nonnull final PolicyUpdateInput policyInput,
      @Nonnull final DataHubPolicyInfo result) {
    if (policyInput.getDescription() != null) {
      result.setDescription(policyInput.getDescription());
    }
    result.setType(policyInput.getType().toString());
    result.setDisplayName(policyInput.getName());
    result.setPrivileges(new StringArray(policyInput.getPrivileges()));
    result.setActors(
        mapActors(
            policyInput.getActors(),
            result.hasActors() ? result.getActors() : new DataHubActorFilter()));
    result.setState(policyInput.getState().toString());
    if (policyInput.getResources() != null) {
      result.setResources(mapResources(policyInput.getResources()));
    } else {
      result.removeResources();
    }
    return result;
  }

  private DataHubActorFilter mapActors(
      final ActorFilterInput actorInput, final DataHubActorFilter result) {
    result.setAllGroups(actorInput.getAllGroups());
    result.setAllUsers(actorInput.getAllUsers());
    result.setResourceOwners(actorInput.getResourceOwners());
    result.setResourceOwnersTypes(
        toUrnArray(actorInput.getResourceOwnersTypes()), SetMode.REMOVE_IF_NULL);
    result.setGroups(toUrnArray(actorInput.getGroups()), SetMode.REMOVE_IF_NULL);
    result.setUsers(toUrnArray(actorInput.getUsers()), SetMode.REMOVE_IF_NULL);
    // `roles` has no representation in ActorFilterInput - deliberately left untouched.
    return result;
  }

  @Nullable
  private UrnArray toUrnArray(@Nullable final List<String> urnStrs) {
    if (urnStrs == null) {
      return null;
    }
    return new UrnArray(urnStrs.stream().map(this::createUrn).collect(Collectors.toList()));
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
    if (resourceInput.getPrivilegeConstraints() != null) {
      result.setPrivilegeConstraints(mapFilter(resourceInput.getPrivilegeConstraints()));
    }

    return result;
  }

  private PolicyMatchFilter mapFilter(final PolicyMatchFilterInput filter) {
    return new PolicyMatchFilter()
        .setCriteria(
            new PolicyMatchCriterionArray(
                filter.getCriteria().stream()
                    .map(this::mapCriterion)
                    .collect(Collectors.toList())));
  }

  private PolicyMatchCriterion mapCriterion(
      final com.linkedin.datahub.graphql.generated.PolicyMatchCriterionInput criterion) {
    final PolicyMatchCriterion result =
        new PolicyMatchCriterion()
            .setField(criterion.getField())
            .setValues(new StringArray(criterion.getValues()))
            .setCondition(PolicyMatchCondition.valueOf(criterion.getCondition().name()));

    // Map structured property values if present
    if (criterion.getStructuredPropertyValues() != null
        && !criterion.getStructuredPropertyValues().isEmpty()) {
      final StructuredPropertyCriterionValueArray structuredPropValues =
          new StructuredPropertyCriterionValueArray(
              criterion.getStructuredPropertyValues().stream()
                  .map(
                      propValue ->
                          new StructuredPropertyCriterionValue()
                              .setPropertyUrn(createUrn(propValue.getPropertyUrn()))
                              .setValues(new StringArray(propValue.getValues())))
                  .collect(Collectors.toList()));
      result.setStructuredPropertyValues(structuredPropValues);
    }

    return result;
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
