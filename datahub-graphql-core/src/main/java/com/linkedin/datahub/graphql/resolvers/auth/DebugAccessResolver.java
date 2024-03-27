package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.DebugAccessResult;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DebugAccessResolver implements DataFetcher<CompletableFuture<DebugAccessResult>> {

  private static final String LAST_UPDATED_AT_FIELD = "lastUpdatedTimestamp";
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

          return populateDebugAccessResult(userUrn, context);
        });
  }

  public DebugAccessResult populateDebugAccessResult(String userUrn, QueryContext context) {

    try {
      final String actorUrn = context.getActorUrn();
      final DebugAccessResult result = new DebugAccessResult();
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

      result.setRoles(roles);
      result.setGroups(groups);
      result.setGroupsWithRoles(groupsWithRoles);
      result.setRolesViaGroups(new ArrayList<>(rolesViaGroups));
      result.setAllRoles(new ArrayList<>(allRoles));

      // TODO Filter for policy URNs that apply to user, group, role URNs

      // List of Policy that apply to this user directly or indirectly.
      result.setPolicies(getPoliciesFor(context, userUrn, groups, roles));

      // List of privileges that this user has directly or indirectly.
      // result.setPrivileges();
      return result;
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

  private List<String> getPoliciesFor(
      final QueryContext context,
      final String user,
      final List<String> groups,
      final List<String> roles)
      throws RemoteInvocationException {
    final SortCriterion sortCriterion =
        new SortCriterion().setField(LAST_UPDATED_AT_FIELD).setOrder(SortOrder.DESCENDING);
    SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            Constants.POLICY_ENTITY_NAME,
            "",
            buildFilterToGetPolicies(user, groups, roles),
            sortCriterion,
            0,
            10000);
    return null;
  }

  private Filter buildFilterToGetPolicies(
      final String user, final List<String> groups, final List<String> roles) {
    final AndFilterInput globalCriteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();
    andConditions.add(
        new FacetFilterInput(
            "actors/users", null, ImmutableList.of(user), false, FilterOperator.EQUAL));
    globalCriteria.setAnd(andConditions);
    return buildFilter(Collections.emptyList(), ImmutableList.of(globalCriteria));
  }
}
