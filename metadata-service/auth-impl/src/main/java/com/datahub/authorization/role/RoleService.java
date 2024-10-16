package com.datahub.authorization.role;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.RoleMembership;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
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
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> actors,
      @Nullable final Urn roleUrn)
      throws RemoteInvocationException {
    if (roleUrn != null && !_entityClient.exists(opContext, roleUrn)) {
      throw new RuntimeException(
          String.format("Role %s does not exist. Skipping batch role assignment", roleUrn));
    }
    actors.forEach(
        actor -> {
          try {
            assignRoleToActor(opContext, actor, roleUrn);
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
      @Nonnull OperationContext opContext, @Nonnull final String actor, @Nullable final Urn roleUrn)
      throws URISyntaxException, RemoteInvocationException {
    final Urn actorUrn = Urn.createFromString(actor);
    if (!_entityClient.exists(opContext, actorUrn)) {
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
    _entityClient.ingestProposal(opContext, proposal, false);
  }
}
