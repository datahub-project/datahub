package com.linkedin.datahub.graphql.resolvers;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PlatformPrivileges;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.datahub.graphql.generated.AuthenticatedUser;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserSnapshotMapper;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * GraphQL resolver responsible for resolving information about the currently
 * logged in User, including
 *
 *    1. User profile information
 *    2. User privilege information, i.e. which features to display in the UI.
 *
 */
public class MeResolver implements DataFetcher<CompletableFuture<AuthenticatedUser>> {

  private final RestliEntityClient _entityClient;

  public MeResolver(final RestliEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<AuthenticatedUser> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    return CompletableFuture.supplyAsync(() -> {
      try {
        // 1. Get currently logged in user profile.
        final Urn userUrn = Urn.createFromString(context.getActor());
        final CorpUserSnapshot gmsUser = _entityClient.get(userUrn, userUrn.toString())
            .getValue()
            .getCorpUserSnapshot();
        final CorpUser corpUser = CorpUserSnapshotMapper.map(gmsUser);

        // 2. Get platform privileges
        final PlatformPrivileges platformPrivileges = new PlatformPrivileges();
        platformPrivileges.setViewAnalytics(canViewAnalytics(context));
        platformPrivileges.setManagePolicies(canManagePolicies(context));
        platformPrivileges.setManageIdentities(canManageUsersGroups(context));

        // Construct and return authenticated user object.
        final AuthenticatedUser authUser = new AuthenticatedUser();
        authUser.setCorpUser(corpUser);
        authUser.setPlatformPrivileges(platformPrivileges);
        return authUser;
      } catch (URISyntaxException | RemoteInvocationException e) {
        throw new RuntimeException("Failed to fetch authenticated user!", e);
      }
    });
  }

  /**
   * Returns true if the authenticated user has privileges to view analytics.
   */
  private boolean canViewAnalytics(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActor(), PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE);
  }

  /**
   * Returns true if the authenticated user has privileges to manage policies analytics.
   */
  private boolean canManagePolicies(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActor(), PoliciesConfig.MANAGE_POLICIES_PRIVILEGE);
  }

  /**
   * Returns true if the authenticated user has privileges to manage users & groups.
   */
  private boolean canManageUsersGroups(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActor(), PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE);
  }

  /**
   * Returns true if the the provided actor is authorized for a particular privilege, false otherwise.
   */
  private boolean isAuthorized(final Authorizer authorizer, String actor, PoliciesConfig.Privilege privilege) {
    final AuthorizationRequest request = new AuthorizationRequest(actor, privilege.getType(), Optional.empty());
    final AuthorizationResult result = authorizer.authorize(request);
    return AuthorizationResult.Type.ALLOW.equals(result.getType());
  }
}
