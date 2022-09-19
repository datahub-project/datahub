package com.linkedin.datahub.graphql.resolvers.user;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.InviteToken;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;

/**
 * Resolver responsible for creating an invite token that Admins can share with prospective users to create native
 * user accounts.
 */
@RequiredArgsConstructor
@Deprecated
public class CreateNativeUserInviteTokenResolver implements DataFetcher<CompletableFuture<InviteToken>> {
  private final InviteTokenService _inviteTokenService;

  @Override
  public CompletableFuture<InviteToken> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to create invite tokens. Please contact your DataHub administrator if this needs corrective action.");
    }

    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {

      try {
        return new InviteToken(_inviteTokenService.getInviteToken(null, true, authentication));
      } catch (Exception e) {
        throw new RuntimeException("Failed to create new invite token");
      }
    });
  }
}
