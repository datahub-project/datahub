package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateRoleInput;
import com.linkedin.datahub.graphql.resolvers.policy.PolicyAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubRoleKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateRoleResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (!PolicyAuthUtils.canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final CreateRoleInput input =
        bindArgument(environment.getArgument("input"), CreateRoleInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final DataHubRoleInfo roleInfo = new DataHubRoleInfo();
            roleInfo.setName(input.getName());
            roleInfo.setDescription(input.getDescription() != null ? input.getDescription() : "");
            roleInfo.setEditable(true);

            final DataHubRoleKey roleKey = new DataHubRoleKey();
            roleKey.setId(UUID.randomUUID().toString());

            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    roleKey, DATAHUB_ROLE_ENTITY_NAME, DATAHUB_ROLE_INFO_ASPECT_NAME, roleInfo);

            return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create role with name %s", input.getName()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
