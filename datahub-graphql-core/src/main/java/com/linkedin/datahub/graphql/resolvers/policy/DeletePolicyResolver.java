package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.metadata.authorization.AuthorizationManager;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.RestliEntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;


/**
 * Resolver responsible for hard deleting a particular DataHub access control policy.
 */
public class DeletePolicyResolver implements DataFetcher<CompletableFuture<String>> {

  private final RestliEntityClient _entityClient;

  public DeletePolicyResolver(final RestliEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (PolicyAuthUtils.canManagePolicies(context)) {
      final String policyUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(policyUrn);
      return CompletableFuture.supplyAsync(() -> {
        try {
          _entityClient.deleteEntity(urn, context.getActor());
          if (context.getAuthorizer() instanceof AuthorizationManager) {
            ((AuthorizationManager) context.getAuthorizer()).invalidateCache();
          }
          return policyUrn;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against policy with urn %s", policyUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
