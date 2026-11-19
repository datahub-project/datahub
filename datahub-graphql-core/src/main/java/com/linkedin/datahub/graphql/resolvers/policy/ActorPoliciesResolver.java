package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;

import com.datahub.authorization.ActorPolicies;
import com.datahub.authorization.ActorPolicyMatch;
import com.datahub.authorization.ActorPolicyMatchReason;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ActorPoliciesInput;
import com.linkedin.datahub.graphql.generated.ActorPoliciesResult;
import com.linkedin.datahub.graphql.generated.ActorPoliciesView;
import com.linkedin.datahub.graphql.generated.ActorPolicy;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.DataHubRole;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.generated.PolicyMatchReason;
import com.linkedin.datahub.graphql.generated.PolicyMatchReasonType;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Lists the policies that apply to a CorpUser or CorpGroup, with the reasons each applies. Managers
 * of policies get the FULL view of anyone; a user viewing themself, or a member viewing their
 * group, gets the TRIMMED view (no actors) when self-service is enabled.
 */
public class ActorPoliciesResolver implements DataFetcher<CompletableFuture<ActorPoliciesResult>> {

  private final FeatureFlags featureFlags;

  public ActorPoliciesResolver(@Nonnull final FeatureFlags featureFlags) {
    this.featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<ActorPoliciesResult> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    if (!(context.getAuthorizer() instanceof AuthorizerChain chain)) {
      return CompletableFuture.completedFuture(null);
    }
    final DataHubAuthorizer authorizer = chain.getDefaultAuthorizer();
    final Urn target = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final boolean isGroup = CORP_GROUP_ENTITY_NAME.equals(target.getEntityType());
    final ActorPoliciesInput input =
        environment.getArgument("input") == null
            ? new ActorPoliciesInput()
            : bindArgument(environment.getArgument("input"), ActorPoliciesInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final ActorPoliciesView view = resolveView(context, authorizer, target, isGroup);
          final boolean full = view == ActorPoliciesView.FULL;
          final boolean includeInactive = full && Boolean.TRUE.equals(input.getIncludeInactive());
          final ActorPolicies policies =
              isGroup
                  ? authorizer.explainGroupPolicies(
                      context.getOperationContext(), target, includeInactive)
                  : authorizer.explainUserPolicies(
                      context.getOperationContext(), target, includeInactive);
          return ActorPoliciesResult.builder()
              .setView(view)
              .setPolicies(mapPolicies(context, policies.policies(), full))
              .setOwnerPolicies(mapPolicies(context, policies.ownerPolicies(), full))
              .build();
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private ActorPoliciesView resolveView(
      @Nonnull final QueryContext context,
      @Nonnull final DataHubAuthorizer authorizer,
      @Nonnull final Urn target,
      final boolean isGroup) {
    if (AuthorizationUtils.canManagePolicies(context)) {
      return ActorPoliciesView.FULL;
    }
    if (featureFlags.isActorPoliciesSelfServiceEnabled()) {
      final Urn caller = UrnUtils.getUrn(context.getActorUrn());
      final boolean selfOrMember =
          isGroup
              ? authorizer.getActorGroupUrns(context.getOperationContext(), caller).contains(target)
              : caller.equals(target);
      if (selfOrMember) {
        return ActorPoliciesView.TRIMMED;
      }
    }
    throw new AuthorizationException("Unauthorized to view policies for this user or group");
  }

  private static List<ActorPolicy> mapPolicies(
      @Nonnull final QueryContext context,
      @Nonnull final List<ActorPolicyMatch> matches,
      final boolean full) {
    return matches.stream()
        .filter(
            match ->
                !PolicyInfoPolicyMapper.isUnrepresentable(
                    match.info(), match.builtIn() ? "built-in policy" : match.policyUrn()))
        .map(match -> mapPolicy(context, match, full))
        .toList();
  }

  private static ActorPolicy mapPolicy(
      @Nonnull final QueryContext context,
      @Nonnull final ActorPolicyMatch match,
      final boolean full) {
    final Policy policy = PolicyInfoPolicyMapper.map(context, match.info());
    return ActorPolicy.builder()
        .setUrn(match.policyUrn() == null ? null : match.policyUrn().toString())
        .setBuiltIn(match.builtIn())
        .setName(policy.getName())
        .setDescription(policy.getDescription())
        .setType(policy.getType())
        .setState(policy.getState())
        .setPrivileges(policy.getPrivileges())
        .setResources(policy.getResources())
        .setActors(full ? policy.getActors() : null)
        .setMatchedBy(match.reasons().stream().map(ActorPoliciesResolver::mapReason).toList())
        .build();
  }

  private static PolicyMatchReason mapReason(@Nonnull final ActorPolicyMatchReason reason) {
    return PolicyMatchReason.builder()
        .setType(PolicyMatchReasonType.valueOf(reason.type().name()))
        .setGroup(groupStub(reason.group()))
        .setRole(roleStub(reason.role()))
        .build();
  }

  @Nullable
  private static CorpGroup groupStub(@Nullable final Urn urn) {
    if (urn == null) {
      return null;
    }
    final CorpGroup group = new CorpGroup();
    group.setUrn(urn.toString());
    group.setType(EntityType.CORP_GROUP);
    return group;
  }

  @Nullable
  private static DataHubRole roleStub(@Nullable final Urn urn) {
    if (urn == null) {
      return null;
    }
    final DataHubRole role = new DataHubRole();
    role.setUrn(urn.toString());
    role.setType(EntityType.DATAHUB_ROLE);
    return role;
  }
}
