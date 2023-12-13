package com.datahub.authorization.role;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.RoleMembership;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RoleService {
  private final EntityClient _entityClient;

  public void batchAssignRoleToActors(
      @Nonnull final List<String> actors,
      @Nullable final Urn roleUrn,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    if (roleUrn != null && !_entityClient.exists(roleUrn, authentication)) {
      throw new RuntimeException(
          String.format("Role %s does not exist. Skipping batch role assignment", roleUrn));
    }
    actors.forEach(
        actor -> {
          try {
            assignRoleToActor(actor, roleUrn, authentication);
          } catch (Exception e) {
            log.warn(
                String.format(
                    "Failed to assign role %s to actor %s. Skipping actor assignment",
                    roleUrn, actor),
                e);
          }
        });
  }

  private void assignRoleToActor(
      @Nonnull final String actor,
      @Nullable final Urn roleUrn,
      @Nonnull final Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    final Urn actorUrn = Urn.createFromString(actor);
    if (!_entityClient.exists(actorUrn, authentication)) {
      log.warn(
          String.format(
              "Failed to assign role %s to actor %s, actor does not exist. Skipping actor assignment",
              roleUrn, actor));
      return;
    }

    final RoleMembership roleMembership = new RoleMembership();
    if (roleUrn == null) {
      roleMembership.setRoles(new UrnArray());
    } else {
      roleMembership.setRoles(new UrnArray(roleUrn));
    }

    // Ingest new RoleMembership aspect
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposal(actorUrn, ROLE_MEMBERSHIP_ASPECT_NAME, roleMembership);
    _entityClient.ingestProposal(proposal, authentication, false);
  }
}
