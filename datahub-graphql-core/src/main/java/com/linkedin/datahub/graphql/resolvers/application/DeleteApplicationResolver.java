package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.metadata.Constants.APPLICATION_ENTITY_NAME;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteApplicationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ApplicationService applicationService;

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn applicationUrn = UrnUtils.getUrn(environment.getArgument("urn"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // The privilege below is checked against the application type, so a URN of any other type
          // would be deleted under a policy that never covered it.
          final DisjunctivePrivilegeGroup orPrivilegeGroup =
              new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));
          if (!APPLICATION_ENTITY_NAME.equals(applicationUrn.getEntityType())
              || !AuthorizationUtils.isAuthorized(
                  context, APPLICATION_ENTITY_NAME, applicationUrn.toString(), orPrivilegeGroup)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          // Checked after authorization so the response does not reveal whether the urn exists.
          if (!applicationService.verifyEntityExists(
              context.getOperationContext(), applicationUrn)) {
            throw new IllegalArgumentException("The Application provided does not exist");
          }

          try {
            final var references =
                applicationService.deleteApplicationThenReferences(
                    context.getOperationContext(), applicationUrn);
            // The entity is already gone. This removes the remaining references.
            CompletableFuture.runAsync(
                () -> {
                  try {
                    references.run();
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Caught exception while attempting to clear all entity references for Application with urn %s",
                            applicationUrn),
                        e);
                  }
                });
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to delete Application", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
