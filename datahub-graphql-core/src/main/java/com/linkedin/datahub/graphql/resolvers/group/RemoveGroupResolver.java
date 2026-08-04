package com.linkedin.datahub.graphql.resolvers.group;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for hard deleting a particular DataHub Corp Group */
@Slf4j
public class RemoveGroupResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final GroupService _groupService;

  public RemoveGroupResolver(final EntityClient entityClient, final GroupService groupService) {
    _entityClient = entityClient;
    _groupService = groupService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final String groupUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(groupUrn);
      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // Capture members BEFORE the delete. deleteEntity's key-aspect DELETE MCL leads to
              // graphService.removeNode(urn), which reaps incoming IsMemberOfNativeGroup edges
              // even though each edge is owned by a member's nativeGroupMembership aspect.
              // Discovering referrers afterwards, as deleteEntityReferences does by scrolling
              // incoming edges, races that removal and loses, stranding a dangling group URN on
              // every member.
              //
              // This capture is its own try/catch, isolated from the delete itself, because a
              // single unparseable URN in the graph index makes getRelatedEntities throw - and
              // without this isolation that would make the group permanently undeletable (every
              // retry hits the same corrupt edge). Falling back to an empty list here degrades to
              // exactly the pre-capture behavior: the delete still succeeds, deleteEntityReferences
              // still attempts its own cleanup, and a member who is re-added later repairs their
              // own edge via GroupService's restoreIndices path.
              final List<Urn> capturedMembers = captureMembersBeforeDelete(context, urn);

              _entityClient.deleteEntity(context.getOperationContext(), urn);

              // Asynchronously Delete all references to the entity (to return quickly)
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      cleanUpNativeGroupMembership(context, urn, capturedMembers);
                    } catch (Exception e) {
                      log.error(
                          String.format(
                              "Caught exception while clearing native group membership for group with urn %s",
                              urn),
                          e);
                    }
                    try {
                      _entityClient.deleteEntityReferences(context.getOperationContext(), urn);
                    } catch (Exception e) {
                      log.error(
                          String.format(
                              "Caught exception while attempting to clear all entity references for group with urn %s",
                              urn),
                          e);
                    }
                  });

              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform delete against group with urn %s", groupUrn), e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<Urn> captureMembersBeforeDelete(final QueryContext context, final Urn groupUrn) {
    try {
      return _groupService.getNativeGroupMembers(groupUrn, context.getActorUrn());
    } catch (Exception e) {
      log.warn(
          "Failed to capture native group members for group {} before delete; member-side "
              + "nativeGroupMembership cleanup will be skipped for this delete. A member "
              + "re-added to a group with this urn later will have their edge repaired "
              + "automatically.",
          groupUrn,
          e);
      return List.of();
    }
  }

  private void cleanUpNativeGroupMembership(
      final QueryContext context, final Urn groupUrn, final List<Urn> capturedMembers)
      throws Exception {
    if (capturedMembers.isEmpty()) {
      return;
    }
    // The captured list predates the delete. If the group has since been recreated, applying it
    // would strip memberships that a subsequent addGroupMembers legitimately restored — undoing a
    // deliberate admin action.
    if (_groupService.groupExists(context.getOperationContext(), groupUrn)) {
      log.info(
          "Group {} exists again; skipping native group membership cleanup for {} captured member(s).",
          groupUrn,
          capturedMembers.size());
      return;
    }
    _groupService.removeExistingNativeGroupMembers(
        context.getOperationContext(), groupUrn, capturedMembers);
  }
}
