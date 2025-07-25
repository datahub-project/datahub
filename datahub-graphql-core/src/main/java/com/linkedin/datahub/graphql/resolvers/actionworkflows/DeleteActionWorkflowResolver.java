package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteActionWorkflowResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ActionWorkflowService _actionWorkflowService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final DeleteActionWorkflowInput input =
        bindArgument(environment.getArgument("input"), DeleteActionWorkflowInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return deleteActionWorkflow(input, context);
          } catch (Exception e) {
            log.error("Failed to delete action workflow", e);
            throw new RuntimeException("Failed to delete action workflow", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean deleteActionWorkflow(
      @Nonnull final DeleteActionWorkflowInput input, @Nonnull final QueryContext context)
      throws Exception {

    // Check authorization - user must have MANAGE_ACTION_WORKFLOWS privilege
    if (!ActionRequestUtils.canManageActionWorkflows(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final Urn workflowUrn = UrnUtils.getUrn(input.getUrn());

    // Delete the workflow using the service
    _actionWorkflowService.deleteActionWorkflow(context.getOperationContext(), workflowUrn);

    return true;
  }
}
