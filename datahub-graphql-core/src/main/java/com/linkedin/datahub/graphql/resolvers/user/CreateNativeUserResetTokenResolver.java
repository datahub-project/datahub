package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.user.NativeUserService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateNativeUserResetTokenInput;
import com.linkedin.datahub.graphql.generated.ResetToken;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for creating a password reset token that Admins can share with native users
 * to reset their credentials.
 */
public class CreateNativeUserResetTokenResolver
    implements DataFetcher<CompletableFuture<ResetToken>> {
  private final NativeUserService _nativeUserService;

  public CreateNativeUserResetTokenResolver(final NativeUserService nativeUserService) {
    _nativeUserService = nativeUserService;
  }

  @Override
  public CompletableFuture<ResetToken> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateNativeUserResetTokenInput input =
        bindArgument(environment.getArgument("input"), CreateNativeUserResetTokenInput.class);

    final String userUrnString = input.getUserUrn();
    Objects.requireNonNull(userUrnString, "No user urn was provided!");

    if (!canManageUserCredentials(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String resetToken =
                _nativeUserService.generateNativeUserPasswordResetToken(
                    userUrnString, context.getAuthentication());
            return new ResetToken(resetToken);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to generate password reset token for user: %s", userUrnString));
          }
        });
  }
}
