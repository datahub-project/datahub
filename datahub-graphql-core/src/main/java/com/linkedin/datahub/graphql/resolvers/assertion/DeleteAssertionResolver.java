package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.metadata.aspect.utils.AssertionUtils.getEntityFromAssertionInfo;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver that deletes an Assertion. */
@Slf4j
public class DeleteAssertionResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;

  public DeleteAssertionResolver(
      final EntityClient entityClient, final EntityService<?> entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn assertionUrn = Urn.createFromString(environment.getArgument("urn"));
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Authorize before the existence check so the response does not reveal whether the urn
          // exists.
          if (!isAuthorizedToDeleteAssertion(context, assertionUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          if (!_entityService.exists(context.getOperationContext(), assertionUrn, true)) {
            return true;
          }

          try {
            final var references =
                _entityClient.deleteReferencesThenEntity(
                    context.getOperationContext(), assertionUrn);

            // The entity is already gone. This removes the remaining references.
            CompletableFuture.runAsync(
                () -> {
                  try {
                    references.run();
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Caught exception while attempting to clear all entity references for assertion with urn %s",
                            assertionUrn),
                        e);
                  }
                });

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to perform delete against assertion with urn %s", assertionUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Determine whether the current user is allowed to remove an assertion. */
  private boolean isAuthorizedToDeleteAssertion(
      final QueryContext context, final Urn assertionUrn) {
    if (!AuthorizationUtils.isEntityType(assertionUrn, Constants.ASSERTION_ENTITY_NAME)) {
      return false;
    }

    AssertionInfo info =
        (AssertionInfo)
            EntityUtils.getAspectFromEntity(
                context.getOperationContext(),
                assertionUrn.toString(),
                Constants.ASSERTION_INFO_ASPECT_NAME,
                _entityService,
                null);

    if (info != null) {
      final Urn asserteeUrn = getEntityFromAssertionInfo(info);
      if (asserteeUrn != null) {
        return AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn);
      }
    }

    // Without an assertee there is nothing else to authorize against.
    return AuthorizationUtils.canDeleteEntity(assertionUrn, context);
  }
}
