package com.datahub.authorization.role;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
public class RoleService {
  private static final String HASHING_ALGORITHM = "SHA-256";
  private final EntityClient _entityClient;

  public RoleService(@Nonnull final EntityClient entityClient) {
    _entityClient = entityClient;
  }

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
