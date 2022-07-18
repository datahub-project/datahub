package com.linkedin.datahub.graphql.resolvers.user;

import com.datahub.authentication.user.NativeUserService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.InviteToken;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;

/**
 * Resolver responsible for creating an invite token that Admins can share with prospective users to create native
 * user accounts.
 */
public class CreateNativeUserInviteTokenResolver implements DataFetcher<CompletableFuture<InviteToken>> {
  private final NativeUserService _nativeUserService;

  public CreateNativeUserInviteTokenResolver(final NativeUserService nativeUserService) {
    _nativeUserService = nativeUserService;
  }

  @Override
  public CompletableFuture<InviteToken> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    return CompletableFuture.supplyAsync(() -> {
      if (!canManageUserCredentials(context)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      try {
        String inviteToken = _nativeUserService.generateNativeUserInviteToken(context.getAuthentication());
        return new InviteToken(inviteToken);
      } catch (Exception e) {
        throw new RuntimeException("Failed to generate new invite token");
      }
    });
  }
}
