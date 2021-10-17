package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateGroupInput;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


// Currently, this resolver will override the group details, but not group membership, if a group with the same name already exists.
public class CreateGroupResolver implements DataFetcher<CompletableFuture<String>> {

  private final AspectClient _aspectClient;

  public CreateGroupResolver(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final CreateGroupInput input = bindArgument(environment.getArgument("input"), CreateGroupInput.class);

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, check if the group already exists.
          // Create the Group key.
          final CorpGroupKey key = new CorpGroupKey();
          key.setName(input.getName());

          // Create the Group info.
          final CorpGroupInfo info = new CorpGroupInfo();
          info.setDisplayName(input.getName());
          info.setDescription(input.getDescription());
          info.setGroups(new CorpGroupUrnArray());
          info.setMembers(new CorpuserUrnArray());
          info.setAdmins(new CorpuserUrnArray());

          // Finally, create the MetadataChangeProposal.
          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));
          proposal.setEntityType(Constants.CORP_GROUP_ENTITY_NAME);
          proposal.setAspectName(Constants.CORP_GROUP_INFO_ASPECT_NAME);
          proposal.setAspect(GenericAspectUtils.serializeAspect(info));
          proposal.setChangeType(ChangeType.UPSERT);
          return _aspectClient.ingestProposal(proposal, context.getActor()).getEntity();
        } catch (Exception e) {
          throw new RuntimeException("Failed to create group", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}