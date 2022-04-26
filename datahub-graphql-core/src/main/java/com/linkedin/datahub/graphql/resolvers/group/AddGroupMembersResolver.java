package com.linkedin.datahub.graphql.resolvers.group;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AddGroupMembersInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Resolver that adds a set of members to a group, if the user and group both exist.
 */
public class AddGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public AddGroupMembersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    final AddGroupMembersInput input = bindArgument(environment.getArgument("input"), AddGroupMembersInput.class);
    final QueryContext context = environment.getContext();

    if (isAuthorized(input, context)) {
      final String groupUrnStr = input.getGroupUrn();
      final List<String> userUrnStrs = input.getUserUrns();

      return CompletableFuture.runAsync(() -> {
        if (!groupExists(groupUrnStr, context)) {
          // The group doesn't exist.
          throw new DataHubGraphQLException("Failed to add member to group. Group does not exist.", DataHubGraphQLErrorCode.NOT_FOUND);
        }
      })
      .thenApply(ignored -> CompletableFuture.allOf(
          userUrnStrs.stream().map(userUrnStr -> CompletableFuture.runAsync(() ->
              addUserToGroup(userUrnStr, groupUrnStr, context)
          )).toArray(CompletableFuture[]::new)))
      .thenApply((ignored) -> Boolean.TRUE);
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorized(AddGroupMembersInput input, QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        CORP_GROUP_ENTITY_NAME,
        input.getGroupUrn(),
        orPrivilegeGroups);
  }

  private void addUserToGroup(final String userUrnStr, final String groupUrnStr, final QueryContext context) {
    try {
      // First, fetch user's group membership aspect.
      Urn userUrn = Urn.createFromString(userUrnStr);
      final EntityResponse entityResponse =
          _entityClient.batchGetV2(CORP_USER_ENTITY_NAME, Collections.singleton(userUrn),
              Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME), context.getAuthentication()).get(userUrn);

      GroupMembership groupMembership;
      if (entityResponse == null || !entityResponse.getAspects().containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
        // Verify the user exists
        if (!userExists(userUrnStr, context)) {
          throw new DataHubGraphQLException("Failed to add member to group. User does not exist.", DataHubGraphQLErrorCode.NOT_FOUND);
        }
        // If the user doesn't have one, create one.
        groupMembership = new GroupMembership();
        groupMembership.setGroups(new UrnArray());
      } else {
        groupMembership = new GroupMembership(entityResponse.getAspects()
            .get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
      }
      // Handle the duplicate case.
      final Urn groupUrn = Urn.createFromString(groupUrnStr);
      groupMembership.getGroups().remove(groupUrn);
      groupMembership.getGroups().add(groupUrn);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();
      proposal.setEntityUrn(Urn.createFromString(userUrnStr));
      proposal.setEntityType(CORP_USER_ENTITY_NAME);
      proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
      proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
      proposal.setChangeType(ChangeType.UPSERT);
      _entityClient.ingestProposal(proposal, context.getAuthentication());
    } catch (Exception e) {
      throw new RuntimeException("Failed to add member to group", e);
    }
  }

  private boolean groupExists(final String groupUrnStr, final QueryContext context) {
    try {
      Urn groupUrn = Urn.createFromString(groupUrnStr);
      final EntityResponse entityResponse = _entityClient.batchGetV2(
          CORP_GROUP_ENTITY_NAME,
          Collections.singleton(groupUrn),
          Collections.singleton(CORP_GROUP_KEY_ASPECT_NAME),
          context.getAuthentication()).get(groupUrn);
      return entityResponse != null && entityResponse.getAspects().containsKey(CORP_GROUP_KEY_ASPECT_NAME);
    } catch (Exception e) {
      throw new DataHubGraphQLException("Failed to fetch group!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }

  private boolean userExists(final String userUrnStr, final QueryContext context) {
    try {
      Urn userUrn = Urn.createFromString(userUrnStr);
      final EntityResponse entityResponse = _entityClient.batchGetV2(
          CORP_USER_ENTITY_NAME,
          Collections.singleton(userUrn),
          Collections.singleton(CORP_USER_KEY_ASPECT_NAME),
          context.getAuthentication()).get(userUrn);
      return entityResponse != null && entityResponse.getAspects().containsKey(CORP_USER_KEY_ASPECT_NAME);
    } catch (Exception e) {
      throw new DataHubGraphQLException("Failed to fetch user!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }
}