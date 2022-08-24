package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleToActorsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.entity.EntityService;
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

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class BatchAssignRoleToActors implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final BatchAssignRoleToActorsInput input =
        bindArgument(environment.getArgument("input"), BatchAssignRoleToActorsInput.class);
    final String roleUrnStr = input.getRoleUrn();
    final List<String> actors = input.getActors();
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {

      try {
        Urn roleUrn = Urn.createFromString(roleUrnStr);
        if (!_entityService.exists(roleUrn)) {
          throw new RuntimeException(String.format("Role %s does not exist", roleUrnStr));
        }
        actors.forEach(actor -> {
          try {
            assignRoleToActor(actor, roleUrn, authentication);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to assign role to actor %s", actor));
          }
        });
        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input, e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    });
  }

  private void assignRoleToActor(String actor, Urn roleUrn, Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    Urn actorUrn = Urn.createFromString(actor);
    if (!_entityService.exists(actorUrn)) {
      log.error(String.format("Actor %s does not exist", actor));
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
