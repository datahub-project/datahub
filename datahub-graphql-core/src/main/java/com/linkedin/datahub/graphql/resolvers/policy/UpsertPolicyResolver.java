package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyUpdateInputInfoMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubPolicyKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpsertPolicyResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  private static final String POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  private final EntityClient _entityClient;

  public UpsertPolicyResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (PolicyAuthUtils.canManagePolicies(context)) {

      final Optional<String> policyUrn = Optional.ofNullable(environment.getArgument("urn"));
      final PolicyUpdateInput input =
          bindArgument(environment.getArgument("input"), PolicyUpdateInput.class);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal;
      final DataHubPolicyInfo info;

      if (policyUrn.isPresent()) {
        // Update existing policy. Mutate the currently persisted aspect in place - rather than
        // building a fresh one from `input` alone - so any field with no representation in
        // PolicyUpdateInput (e.g. `actors.roles`, `editable`) survives the edit instead of being
        // silently reset.
        final Urn urn = Urn.createFromString(policyUrn.get());
        final DataHubPolicyInfo existingInfo = getExistingPolicyInfo(context, urn);
        info =
            PolicyUpdateInputInfoMapper.mapToExisting(
                context, input, existingInfo != null ? existingInfo : new DataHubPolicyInfo());
        info.setLastUpdatedTimestamp(System.currentTimeMillis());
        proposal = buildMetadataChangeProposalWithUrn(urn, POLICY_INFO_ASPECT_NAME, info);
      } else {
        // Create new policy
        info = PolicyUpdateInputInfoMapper.map(context, input);
        info.setLastUpdatedTimestamp(System.currentTimeMillis());

        // Since we are creating a new Policy, we need to generate a unique UUID.
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();

        // Create the Policy key.
        final DataHubPolicyKey key = new DataHubPolicyKey();
        key.setId(uuidStr);
        proposal =
            buildMetadataChangeProposalWithKey(
                key, POLICY_ENTITY_NAME, POLICY_INFO_ASPECT_NAME, info);
      }

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              String urn =
                  _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
              if (context.getAuthorizer() instanceof AuthorizerChain) {
                ((AuthorizerChain) context.getAuthorizer())
                    .getDefaultAuthorizer()
                    .invalidateCache();
              }
              return urn;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform update against input %s", input), e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  @Nullable
  private DataHubPolicyInfo getExistingPolicyInfo(
      @Nonnull final QueryContext context, @Nonnull final Urn urn) throws Exception {
    final EntityResponse response =
        _entityClient.getV2(
            context.getOperationContext(),
            POLICY_ENTITY_NAME,
            urn,
            Collections.singleton(POLICY_INFO_ASPECT_NAME));
    if (response != null && response.getAspects().containsKey(POLICY_INFO_ASPECT_NAME)) {
      return new DataHubPolicyInfo(
          response.getAspects().get(POLICY_INFO_ASPECT_NAME).getValue().data());
    }
    return null;
  }
}
