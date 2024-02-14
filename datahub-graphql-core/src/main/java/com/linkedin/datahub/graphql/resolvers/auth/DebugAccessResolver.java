package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DebugAccessResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DebugAccessResolver implements DataFetcher<CompletableFuture<DebugAccessResult>> {

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public DebugAccessResolver(EntityClient entityClient, GraphClient graphClient) {
    _entityClient = entityClient;
    _graphClient = graphClient;
  }

  @Override
  public CompletableFuture<DebugAccessResult> get(DataFetchingEnvironment environment)
      throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();

          if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          final String userUrn = environment.getArgument("userUrn");

          DebugAccessResult debugAccessResult = new DebugAccessResult();
          populateDebugAccessResult(debugAccessResult, userUrn, context.getActorUrn());
          return debugAccessResult;
        });
  }

  public void populateDebugAccessResult(
      DebugAccessResult debugAccessResult, String userUrn, String actorUrn) {
    try {
      final List<String> types =
          Arrays.asList("IsMemberOfRole", "IsMemberOfGroup", "IsMemberOfNativeGroup");
      EntityRelationships entityRelationships = getEntityRelationships(userUrn, types, actorUrn);

      List<String> roles =
          getUrnsFromEntityRelationships(entityRelationships, Constants.DATAHUB_ROLE_ENTITY_NAME);

      List<String> groups =
          getUrnsFromEntityRelationships(entityRelationships, Constants.CORP_GROUP_ENTITY_NAME);
      List<String> groupsWithRoles = new ArrayList<>();

      Set<String> rolesViaGroups = new HashSet<>();
      groups.forEach(
          groupUrn -> {
            EntityRelationships groupRelationships =
                getEntityRelationships(groupUrn, List.of("IsMemberOfRole"), actorUrn);
            List<String> rolesOfGroup =
                getUrnsFromEntityRelationships(
                    groupRelationships, Constants.DATAHUB_ROLE_ENTITY_NAME);
            if (rolesOfGroup.isEmpty()) {
              return;
            }
            groupsWithRoles.add(groupUrn);
            rolesViaGroups.addAll(rolesOfGroup);
          });
      Set<String> allRoles = new HashSet<>(roles);
      allRoles.addAll(rolesViaGroups);

      debugAccessResult.setRoles(roles);
      debugAccessResult.setGroups(groups);
      debugAccessResult.setGroupsWithRoles(groupsWithRoles);
      debugAccessResult.setRolesViaGroups(new ArrayList<>(rolesViaGroups));
      debugAccessResult.setAllRoles(new ArrayList<>(allRoles));

      // TODO Filter for policy URNs that apply to user, group, role URNs

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getUrnsFromEntityRelationships(
      EntityRelationships entityRelationships, String entityName) {
    return entityRelationships.getRelationships().stream()
        .map(EntityRelationship::getEntity)
        .filter(entity -> entityName.equals(entity.getEntityType()))
        .map(Urn::toString)
        .collect(Collectors.toList());
  }

  private EntityRelationships getEntityRelationships(
      final String urn, final List<String> types, final String actor) {
    return _graphClient.getRelatedEntities(
        urn, types, RelationshipDirection.OUTGOING, 0, 100, actor);
  }
}
