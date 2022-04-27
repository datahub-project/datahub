package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PolicyUpdateInput;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyUpdateInputInfoMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.DataHubPolicyKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

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
      final PolicyUpdateInput input = bindArgument(environment.getArgument("input"), PolicyUpdateInput.class);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();

      if (policyUrn.isPresent()) {
        // Update existing policy
        proposal.setEntityUrn(Urn.createFromString(policyUrn.get()));
      } else {
        // Create new policy
        // Since we are creating a new Policy, we need to generate a unique UUID.
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();

        // Create the Policy key.
        final DataHubPolicyKey key = new DataHubPolicyKey();
        key.setId(uuidStr);
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
      }

      // Create the policy info.
      final DataHubPolicyInfo info = PolicyUpdateInputInfoMapper.map(input);
      info.setLastUpdatedTimestamp(System.currentTimeMillis());

      proposal.setEntityType(POLICY_ENTITY_NAME);
      proposal.setAspectName(POLICY_INFO_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(info));
      proposal.setChangeType(ChangeType.UPSERT);

      return CompletableFuture.supplyAsync(() -> {
        try {
          // TODO: We should also provide SystemMetadata.
          String urn = _entityClient.ingestProposal(proposal, context.getAuthentication());
          if (context.getAuthorizer() instanceof AuthorizerChain) {
            ((AuthorizerChain) context.getAuthorizer()).getDefaultAuthorizer().invalidateCache();
          }
          return urn;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
