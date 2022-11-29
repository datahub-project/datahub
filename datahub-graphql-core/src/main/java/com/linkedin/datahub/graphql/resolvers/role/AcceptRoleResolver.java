package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j

@RequiredArgsConstructor
public class AcceptRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final RoleService _roleService;
  private final InviteTokenService _inviteTokenService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final AcceptRoleInput input = bindArgument(environment.getArgument("input"), AcceptRoleInput.class);
    final String inviteTokenStr = input.getInviteToken();
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Urn inviteTokenUrn = _inviteTokenService.getInviteTokenUrn(inviteTokenStr);
        if (!_inviteTokenService.isInviteTokenValid(inviteTokenUrn, authentication)) {
          throw new RuntimeException(String.format("Invite token %s is invalid", inviteTokenStr));
        }

        final Urn roleUrn = _inviteTokenService.getInviteTokenRole(inviteTokenUrn, authentication);
        _roleService.batchAssignRoleToActors(Collections.singletonList(authentication.getActor().toUrnStr()), roleUrn,
            authentication);

        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to accept role using invite token %s", inviteTokenStr), e);
      }
    });
  }
}
