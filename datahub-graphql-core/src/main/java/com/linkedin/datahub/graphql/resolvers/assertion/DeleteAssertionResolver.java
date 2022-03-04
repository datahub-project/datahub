package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.AuthUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;


/**
 * GraphQL Resolver that deletes an Assertion.
 */
public class DeleteAssertionResolver implements DataFetcher<CompletableFuture<Boolean>>  {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  public DeleteAssertionResolver(final EntityClient entityClient, final EntityService entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn assertionUrn = Urn.createFromString(environment.getArgument("urn"));
    return CompletableFuture.supplyAsync(() -> {

      // 1. check the entity exists. If not, return false.
      if (!_entityService.exists(assertionUrn)) {
        return true;
      }

      if (isAuthorizedToDeleteAssertion(context, assertionUrn)) {
          try {
            _entityClient.deleteEntity(assertionUrn, context.getAuthentication());
            return true;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to perform delete against assertion with urn %s", assertionUrn), e);
          }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  /**
   * Determine whether the current user is allowed to remove an assertion.
   */
  private boolean isAuthorizedToDeleteAssertion(final QueryContext context, final Urn assertionUrn) {

    // 2. fetch the assertion info
    AssertionInfo info =
        (AssertionInfo) MutationUtils.getAspectFromEntity(
            assertionUrn.toString(), Constants.ASSERTION_INFO_ASPECT_NAME, _entityService, null);

    if (info != null) {
      // 3. check whether the actor has permission to edit the assertions on the assertee
      final Urn asserteeUrn = getAsserteeUrnFromInfo(info);
      return isAuthorizedToDeleteAssertionFromAssertee(context, asserteeUrn);
    }

    return true;
  }

  private boolean isAuthorizedToDeleteAssertionFromAssertee(final QueryContext context, final Urn asserteeUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        AuthUtils.ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))
    ));
    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        asserteeUrn.getEntityType(),
        asserteeUrn.toString(),
        orPrivilegeGroups);
  }

  private Urn getAsserteeUrnFromInfo(final AssertionInfo info) {
    switch (info.getType()) {
      case DATASET:
        return info.getDatasetAssertion().getDataset();
      default:
        throw new RuntimeException(String.format("Unsupported Assertion Type %s provided", info.getType()));
    }
  }
}