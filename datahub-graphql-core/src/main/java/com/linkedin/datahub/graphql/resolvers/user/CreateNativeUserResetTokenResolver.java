package com.linkedin.datahub.graphql.resolvers.user;

import com.datahub.authentication.user.NativeUserService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateNativeUserResetTokenInput;
import com.linkedin.datahub.graphql.generated.ResetToken;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver responsible for creating a password reset token that Admins can share with native users to reset their
 * credentials.
 */
public class CreateNativeUserResetTokenResolver implements DataFetcher<CompletableFuture<ResetToken>> {
  private final NativeUserService _nativeUserService;

  public CreateNativeUserResetTokenResolver(final NativeUserService nativeUserService) {
    _nativeUserService = nativeUserService;
  }

  @Override
  public CompletableFuture<ResetToken> get(final DataFetchingEnvironment environment) throws Exception {
    final String condUpdate = environment.getVariables().containsKey(Constants.IN_UNMODIFIED_SINCE)
            ? environment.getVariables().get(Constants.IN_UNMODIFIED_SINCE).toString() : null;
    final QueryContext context = environment.getContext();
    final CreateNativeUserResetTokenInput input =
        bindArgument(environment.getArgument("input"), CreateNativeUserResetTokenInput.class);

    final String userUrnString = input.getUserUrn();
    Objects.requireNonNull(userUrnString, "No user urn was provided!");

    if (!canManageUserCredentials(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
        String resetToken =
            _nativeUserService.generateNativeUserPasswordResetToken(userUrnString, context.getAuthentication(), condUpdate);
        return new ResetToken(resetToken);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to generate password reset token for user: %s", userUrnString));
      }
    });
  }
}