/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/** Resolver responsible for hard deleting a particular DataHub access control policy. */
public class DeletePolicyResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeletePolicyResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (PolicyAuthUtils.canManagePolicies(context)) {
      final String policyUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(policyUrn);
      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              _entityClient.deleteEntity(context.getOperationContext(), urn);
              if (context.getAuthorizer() instanceof AuthorizerChain) {
                ((AuthorizerChain) context.getAuthorizer())
                    .getDefaultAuthorizer()
                    .invalidateCache();
              }
              return policyUrn;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform delete against policy with urn %s", policyUrn),
                  e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
