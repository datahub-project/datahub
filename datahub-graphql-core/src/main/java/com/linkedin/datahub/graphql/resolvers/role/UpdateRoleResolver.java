package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateRoleInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(POLICY_ENTITY_NAME))) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final UpdateRoleInput input =
        bindArgument(environment.getArgument("input"), UpdateRoleInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn roleUrn = Urn.createFromString(input.getUrn());

            // Verify the role exists and is editable.
            final EntityResponse existingRole =
                _entityClient
                    .batchGetV2(
                        context.getOperationContext(),
                        DATAHUB_ROLE_ENTITY_NAME,
                        Collections.singleton(roleUrn),
                        Collections.singleton(DATAHUB_ROLE_INFO_ASPECT_NAME))
                    .get(roleUrn);

            if (existingRole == null) {
              throw new RuntimeException(
                  String.format("Role with urn %s does not exist", input.getUrn()));
            }

            final DataHubRoleInfo existingInfo =
                new DataHubRoleInfo(
                    existingRole.getAspects().get(DATAHUB_ROLE_INFO_ASPECT_NAME).getValue().data());
            if (!existingInfo.isEditable()) {
              throw new RuntimeException(
                  String.format("Role with urn %s is not editable", input.getUrn()));
            }

            final DataHubRoleInfo updatedInfo = new DataHubRoleInfo();
            updatedInfo.setName(input.getName());
            updatedInfo.setDescription(
                input.getDescription() != null ? input.getDescription() : "");
            updatedInfo.setEditable(true);

            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    roleUrn, DATAHUB_ROLE_INFO_ASPECT_NAME, updatedInfo);

            _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update role with urn %s", input.getUrn()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
