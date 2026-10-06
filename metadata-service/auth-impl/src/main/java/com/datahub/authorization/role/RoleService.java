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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RoleService {
  private final EntityClient _entityClient;

  /**
   * Assigns (or with a null role, clears) the role membership of every given actor.
   *
   * <p>Actors are resolved and ingested as a batch: one existence query for all of them and one
   * ingest for all of them, rather than a pair of round trips per actor. Actors that are malformed
   * or do not exist are skipped with a warning, as are individual failures when the batch has to be
   * retried actor by actor.
   */
  public void batchAssignRoleToActors(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> actors,
      @Nullable final Urn roleUrn)
      throws RemoteInvocationException {
    if (roleUrn != null && !_entityClient.exists(opContext, roleUrn)) {
      throw new RuntimeException(
          String.format("Role %s does not exist. Skipping batch role assignment", roleUrn));
    }

    final List<Urn> actorUrns = new ArrayList<>(actors.size());
    for (final String actor : actors) {
      try {
        actorUrns.add(Urn.createFromString(actor));
      } catch (URISyntaxException e) {
        log.warn(
            String.format(
                "Failed to assign role %s to actor %s, actor urn is malformed. Skipping actor assignment",
                roleUrn, actor),
            e);
      }
    }
    if (actorUrns.isEmpty()) {
      return;
    }

    // Resolving existence in one query keeps this at a single round trip regardless of actor count.
    // The check itself cannot be dropped: ingesting a RoleMembership for an actor that does not
    // exist would materialize that entity via its default key aspect, inventing a user.
    final Set<Urn> existingActorUrns = _entityClient.filterExistingUrns(opContext, actorUrns);

    final List<MetadataChangeProposal> proposals = new ArrayList<>(actorUrns.size());
    for (final Urn actorUrn : actorUrns) {
      if (!existingActorUrns.contains(actorUrn)) {
        log.warn(
            String.format(
                "Failed to assign role %s to actor %s, actor does not exist. Skipping actor assignment",
                roleUrn, actorUrn));
        continue;
      }
      proposals.add(buildRoleMembershipProposal(actorUrn, roleUrn));
    }
    if (proposals.isEmpty()) {
      return;
    }

    try {
      _entityClient.batchIngestProposals(opContext, proposals, false);
    } catch (Exception e) {
      log.warn(
          String.format(
              "Failed to assign role %s to %s actors as one batch, retrying them individually",
              roleUrn, proposals.size()),
          e);
      // Retrying per actor preserves the original behaviour that one bad actor does not prevent the
      // rest from being assigned. Re-ingesting is safe because these are idempotent upserts.
      proposals.forEach(proposal -> ingestRoleMembership(opContext, proposal, roleUrn));
    }
  }

  @Nonnull
  private static MetadataChangeProposal buildRoleMembershipProposal(
      @Nonnull final Urn actorUrn, @Nullable final Urn roleUrn) {
    final RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(roleUrn == null ? new UrnArray() : new UrnArray(roleUrn));
    return buildSynchronousMetadataChangeProposal(
        actorUrn, ROLE_MEMBERSHIP_ASPECT_NAME, roleMembership);
  }

  private void ingestRoleMembership(
      @Nonnull final OperationContext opContext,
      @Nonnull final MetadataChangeProposal proposal,
      @Nullable final Urn roleUrn) {
    try {
      _entityClient.ingestProposal(opContext, proposal, false);
    } catch (Exception e) {
      log.warn(
          String.format(
              "Failed to assign role %s to actor %s. Skipping actor assignment",
              roleUrn, proposal.getEntityUrn()),
          e);
    }
  }
}
