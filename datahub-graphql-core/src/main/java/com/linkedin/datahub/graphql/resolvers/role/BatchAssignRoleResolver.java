package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class BatchAssignRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to assign roles. Please contact your DataHub administrator if this needs corrective action.");
    }

    final BatchAssignRoleInput input = bindArgument(environment.getArgument("input"), BatchAssignRoleInput.class);
    final String roleUrnStr = input.getRoleUrn();
    final List<String> actors = input.getActors();
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {
      try {
        Urn roleUrn = Urn.createFromString(roleUrnStr);
        if (!_entityClient.exists(roleUrn, authentication)) {
          throw new RuntimeException(String.format("Role %s does not exist", roleUrnStr));
        }

        actors.forEach(actor -> {
          try {
            assignRoleToActor(actor, roleUrn, authentication);
          } catch (Exception e) {
            log.warn(
                String.format("Failed to assign role %s to actor %s. Skipping actor assignment", roleUrnStr, actor), e);
          }
        });
        return true;
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    });
  }

  private void assignRoleToActor(String actor, Urn roleUrn, Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    Urn actorUrn = Urn.createFromString(actor);
    if (!_entityClient.exists(actorUrn, authentication)) {
      log.warn(String.format("Failed to assign role %s to actor %s, actor does not exist. Skipping actor assignment",
          roleUrn.toString(), actor));
      return;
    }

    RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(new UrnArray(roleUrn));

    // Finally, create the MetadataChangeProposal.
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(actorUrn);
    proposal.setEntityType(CORP_USER_ENTITY_NAME);
    proposal.setAspectName(ROLE_MEMBERSHIP_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(roleMembership));
    proposal.setChangeType(ChangeType.UPSERT);
    _entityClient.ingestProposal(proposal, authentication);
  }
}
