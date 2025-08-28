package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.SendUserInvitationsInput;
import com.linkedin.datahub.graphql.generated.SendUserInvitationsResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SendUserInvitationsResolver
    implements DataFetcher<CompletableFuture<SendUserInvitationsResult>> {
  private final UserInvitationService userInvitationService;

  @Override
  public CompletableFuture<SendUserInvitationsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManagePolicies(context) || !canManageUsersAndGroups(context)) {
      throw new AuthorizationException(
          "Unauthorized to send user invitations. Please contact your DataHub administrator if you need the authorization to manage users/groups and policies.");
    }

    final SendUserInvitationsInput input =
        bindArgument(environment.getArgument("input"), SendUserInvitationsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            userInvitationService.sendUserInvitations(
                context.getOperationContext(), input, context.getAuthentication()),
        this.getClass().getSimpleName(),
        "get");
  }
}
