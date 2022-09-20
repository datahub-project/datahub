package com.datahub.authorization.role;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.RoleMembership;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
@RequiredArgsConstructor
public class RoleService {
  private final EntityClient _entityClient;

  public boolean exists(@Nonnull final Urn urn, final Authentication authentication) throws RemoteInvocationException {
    return _entityClient.exists(urn, authentication);
  }

  public void assignRoleToActor(@Nonnull final String actor, @Nonnull final Urn roleUrn,
      @Nonnull final Authentication authentication) throws URISyntaxException, RemoteInvocationException {
    Urn actorUrn = Urn.createFromString(actor);
    if (!_entityClient.exists(actorUrn, authentication)) {
      log.warn(String.format("Failed to assign role %s to actor %s, actor does not exist. Skipping actor assignment",
          roleUrn, actor));
      return;
    }

    RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(new UrnArray(roleUrn));

    // Ingest new RoleMembership aspect
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposal(actorUrn, ROLE_MEMBERSHIP_ASPECT_NAME, roleMembership);
    _entityClient.ingestProposal(proposal, authentication);
  }
}
