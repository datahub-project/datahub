package com.linkedin.datahub.graphql.resolvers.user;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpUserStatus;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;


/**
 * Resolver responsible for editing a CorpUser's status. Requires the Manage Users & Groups platform privilege.
 */
public class UpdateUserStatusResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public UpdateUserStatusResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {

      final String userUrn = environment.getArgument("urn");
      final CorpUserStatus newStatus = CorpUserStatus.valueOf(environment.getArgument("status"));

      // Create ths status aspect
      final com.linkedin.identity.CorpUserStatus statusAspect = new com.linkedin.identity.CorpUserStatus();
      statusAspect.setStatus(newStatus.toString());
      statusAspect.setLastModified(new AuditStamp().setTime(System.currentTimeMillis()).setActor(Urn.createFromString(context.getActorUrn())));

      return CompletableFuture.supplyAsync(() -> {
        try {
          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          proposal.setEntityUrn(Urn.createFromString(userUrn));
          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to update user status for urn", userUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}