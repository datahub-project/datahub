/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateInviteTokenInput;
import com.linkedin.datahub.graphql.generated.InviteToken;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateInviteTokenResolver implements DataFetcher<CompletableFuture<InviteToken>> {
  private final InviteTokenService _inviteTokenService;

  @Override
  public CompletableFuture<InviteToken> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManageUserCredentials(context)) {
      throw new AuthorizationException(
          "Unauthorized to create invite tokens. Please contact your DataHub administrator if this needs corrective action.");
    }

    final CreateInviteTokenInput input =
        bindArgument(environment.getArgument("input"), CreateInviteTokenInput.class);
    final String roleUrnStr = input.getRoleUrn();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return new InviteToken(
                _inviteTokenService.getInviteToken(
                    context.getOperationContext(), roleUrnStr, true));
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create invite token for role %s", roleUrnStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
