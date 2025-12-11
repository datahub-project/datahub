/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubPolicyKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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

      final DataHubPolicyInfo info = PolicyUpdateInputInfoMapper.map(context, input);
      info.setLastUpdatedTimestamp(System.currentTimeMillis());

      if (policyUrn.isPresent()) {
        // Update existing policy
        proposal =
            buildMetadataChangeProposalWithUrn(
                Urn.createFromString(policyUrn.get()), POLICY_INFO_ASPECT_NAME, info);
      } else {
        // Create new policy
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
}
