package com.linkedin.datahub.graphql.resolvers.group;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveGroupMembersInput;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


public class RemoveGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public RemoveGroupMembersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    final RemoveGroupMembersInput input = bindArgument(environment.getArgument("input"), RemoveGroupMembersInput.class);
    final QueryContext context = environment.getContext();

    if (isAuthorized(input, context)) {
      final Urn groupUrn = Urn.createFromString(input.getGroupUrn());
      final Set<Urn> userUrns = input.getUserUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
      final Map<Urn, EntityResponse> entityResponseMap = _entityClient.batchGetV2(CORP_USER_ENTITY_NAME,
          userUrns, Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME), context.getAuthentication());
      return CompletableFuture.allOf(userUrns.stream().map(userUrn -> CompletableFuture.supplyAsync(() -> {
        try {
          EntityResponse entityResponse = entityResponseMap.get(userUrn);
          if (entityResponse == null) {
            return false;
          }

          final GroupMembership groupMembership =
              new GroupMembership(entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
          if (groupMembership.getGroups().remove(groupUrn)) {
            // Finally, create the MetadataChangeProposal.
            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(userUrn);
            proposal.setEntityType(CORP_USER_ENTITY_NAME);
            proposal.setAspectName(GROUP_MEMBERSHIP_ASPECT_NAME);
            proposal.setAspect(GenericRecordUtils.serializeAspect(groupMembership));
            proposal.setChangeType(ChangeType.UPSERT);
            _entityClient.ingestProposal(proposal, context.getAuthentication());
            return true;
          }

          return true;
        } catch (Exception e) {
          throw new RuntimeException("Failed to remove member from group", e);
        }
      })).toArray(CompletableFuture[]::new)).thenApply(ignored -> Boolean.TRUE);
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorized(RemoveGroupMembersInput input, QueryContext context) {
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
}