package com.linkedin.datahub.graphql.resolvers;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.datahub.authorization.AuthUtil.isAuthorizedEntityType;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.ApiGroup.ANALYTICS;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AuthenticatedUser;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.PlatformPrivileges;
import com.linkedin.datahub.graphql.resolvers.businessattribute.BusinessAttributeAuthorizationUtils;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/**
 * GraphQL resolver responsible for resolving information about the currently logged in User,
 * including
 *
 * <p>1. User profile information 2. User privilege information, i.e. which features to display in
 * the UI.
 */
public class MeResolver implements DataFetcher<CompletableFuture<AuthenticatedUser>> {

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  public MeResolver(final EntityClient entityClient, final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<AuthenticatedUser> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // 1. Get currently logged in user profile.
            final Urn userUrn = Urn.createFromString(context.getActorUrn());
            final EntityResponse gmsUser =
                _entityClient
                    .batchGetV2(
                        context.getOperationContext(),
                        CORP_USER_ENTITY_NAME,
                        Collections.singleton(userUrn),
                        null)
                    .get(userUrn);
            final CorpUser corpUser = CorpUserMapper.map(context, gmsUser, _featureFlags);

            // 2. Get platform privileges
            final PlatformPrivileges platformPrivileges = new PlatformPrivileges();
            platformPrivileges.setViewAnalytics(canViewAnalytics(context));
            platformPrivileges.setManagePolicies(canManagePolicies(context));
            platformPrivileges.setManageIdentities(canManageUsersGroups(context));
            platformPrivileges.setGeneratePersonalAccessTokens(
                canGeneratePersonalAccessToken(context));
            platformPrivileges.setManageDomains(canManageDomains(context));
            platformPrivileges.setManageIngestion(canManageIngestion(context));
            platformPrivileges.setManageSecrets(canManageSecrets(context));
            platformPrivileges.setManageTokens(canManageTokens(context));
            platformPrivileges.setViewTests(canViewTests(context));
            platformPrivileges.setManageTests(canManageTests(context));
            platformPrivileges.setManageGlossaries(canManageGlossaries(context));
            platformPrivileges.setManageUserCredentials(canManageUserCredentials(context));
            platformPrivileges.setCreateDomains(AuthorizationUtils.canCreateDomains(context));
            platformPrivileges.setCreateTags(AuthorizationUtils.canCreateTags(context));
            platformPrivileges.setManageTags(AuthorizationUtils.canManageTags(context));
            platformPrivileges.setManageGlobalViews(
                AuthorizationUtils.canManageGlobalViews(context));
            platformPrivileges.setManageOwnershipTypes(
                AuthorizationUtils.canManageOwnershipTypes(context));
            platformPrivileges.setManageGlobalAnnouncements(
                AuthorizationUtils.canManageGlobalAnnouncements(context));
            platformPrivileges.setCreateBusinessAttributes(
                BusinessAttributeAuthorizationUtils.canCreateBusinessAttribute(context));
            platformPrivileges.setManageBusinessAttributes(
                BusinessAttributeAuthorizationUtils.canManageBusinessAttribute(context));
            platformPrivileges.setManageStructuredProperties(
                AuthorizationUtils.canManageStructuredProperties(context));
            platformPrivileges.setViewStructuredPropertiesPage(
                AuthorizationUtils.canViewStructuredPropertiesPage(context));
            // Construct and return authenticated user object.
            final AuthenticatedUser authUser = new AuthenticatedUser();
            authUser.setCorpUser(corpUser);
            authUser.setPlatformPrivileges(platformPrivileges);
            return authUser;
          } catch (URISyntaxException | RemoteInvocationException e) {
            throw new RuntimeException("Failed to fetch authenticated user!", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Returns true if the authenticated user has privileges to view analytics. */
  private boolean canViewAnalytics(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), ANALYTICS, READ);
  }

  /** Returns true if the authenticated user has privileges to manage policies analytics. */
  private boolean canManagePolicies(final QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(), MANAGE, List.of(POLICY_ENTITY_NAME));
  }

  /** Returns true if the authenticated user has privileges to manage users & groups. */
  private boolean canManageUsersGroups(final QueryContext context) {
    return isAuthorizedEntityType(
        context.getOperationContext(),
        MANAGE,
        List.of(CORP_USER_ENTITY_NAME, CORP_GROUP_ENTITY_NAME));
  }

  /** Returns true if the authenticated user has privileges to generate personal access tokens */
  private boolean canGeneratePersonalAccessToken(final QueryContext context) {
    return isAuthorized(
        context.getOperationContext(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE);
  }

  /** Returns true if the authenticated user has privileges to view tests. */
  private boolean canViewTests(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.VIEW_TESTS_PRIVILEGE);
  }

  /** Returns true if the authenticated user has privileges to manage (add or remove) tests. */
  private boolean canManageTests(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_TESTS_PRIVILEGE);
  }

  /** Returns true if the authenticated user has privileges to manage domains */
  private boolean canManageDomains(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  /** Returns true if the authenticated user has privileges to manage access tokens */
  private boolean canManageTokens(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_ACCESS_TOKENS);
  }

  /** Returns true if the authenticated user has privileges to manage glossaries */
  private boolean canManageGlossaries(final QueryContext context) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE);
  }

  /** Returns true if the authenticated user has privileges to manage user credentials */
  private boolean canManageUserCredentials(@Nonnull QueryContext context) {
    return isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE);
  }
}
