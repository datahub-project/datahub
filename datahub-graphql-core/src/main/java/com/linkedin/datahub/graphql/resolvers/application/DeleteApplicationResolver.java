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
import com.linkedin.entity.client.EntityClient;
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

  private final EntityClient entityClient;
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
          if (!applicationService.verifyEntityExists(
              context.getOperationContext(), applicationUrn)) {
            throw new IllegalArgumentException("The Application provided does not exist");
          }

          final DisjunctivePrivilegeGroup orPrivilegeGroup =
              new DisjunctivePrivilegeGroup(ImmutableList.of(ALL_PRIVILEGES_GROUP));
          if (!AuthorizationUtils.isAuthorized(
              context, APPLICATION_ENTITY_NAME, applicationUrn.toString(), orPrivilegeGroup)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            applicationService.deleteApplication(context.getOperationContext(), applicationUrn);
            CompletableFuture.runAsync(
                () -> {
                  try {
                    this.entityClient.deleteEntityReferences(
                        context.getOperationContext(), applicationUrn);
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
