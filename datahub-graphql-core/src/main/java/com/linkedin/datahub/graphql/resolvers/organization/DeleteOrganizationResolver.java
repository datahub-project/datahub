package com.linkedin.datahub.graphql.resolvers.organization;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteOrganizationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn organizationUrn = UrnUtils.getUrn(urnStr);

    if (!OrganizationAuthUtils.canManageOrganization(context, organizationUrn)) {
      throw new AuthorizationException(
          "Unauthorized to delete organization. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            _entityClient.deleteEntity(context.getOperationContext(), organizationUrn);
            return true;
          } catch (Exception e) {
            log.error("Failed to delete organization", e);
            throw new RuntimeException("Failed to delete organization", e);
          }
        });
  }
}
