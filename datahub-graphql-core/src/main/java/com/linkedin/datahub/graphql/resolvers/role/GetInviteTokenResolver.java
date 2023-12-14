package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetInviteTokenInput;
import com.linkedin.datahub.graphql.generated.InviteToken;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetInviteTokenResolver implements DataFetcher<CompletableFuture<InviteToken>> {
  private final InviteTokenService _inviteTokenService;

  @Override
  public CompletableFuture<InviteToken> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to get invite tokens. Please contact your DataHub administrator if this needs corrective action.");
    }

    final GetInviteTokenInput input =
        bindArgument(environment.getArgument("input"), GetInviteTokenInput.class);
    final String roleUrnStr = input.getRoleUrn();
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return new InviteToken(
                _inviteTokenService.getInviteToken(roleUrnStr, false, authentication));
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to get invite token for role %s", roleUrnStr), e);
          }
        });
  }
}
