package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ReviewActionWorkflowFormRequestResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final ActionWorkflowService _actionWorkflowService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final ReviewActionWorkflowFormRequestInput input =
        bindArgument(environment.getArgument("input"), ReviewActionWorkflowFormRequestInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return reviewActionWorkflowFormRequest(input, context);
          } catch (Exception e) {
            log.error("Failed to review action workflow request", e);
            throw new RuntimeException("Failed to review action workflow request", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean reviewActionWorkflowFormRequest(
      @Nonnull final ReviewActionWorkflowFormRequestInput input,
      @Nonnull final QueryContext context)
      throws Exception {

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final Urn requestUrn = UrnUtils.getUrn(input.getUrn());
    final String result = input.getResult().toString(); // ACCEPTED or REJECTED
    final String comment = input.getComment();

    try {
      return _actionWorkflowService.reviewActionWorkflowFormRequest(
          requestUrn, actorUrn, result, comment, context.getOperationContext());
    } catch (RuntimeException e) {
      // Convert authorization errors to GraphQL authorization exceptions
      if (e.getMessage().contains("is not authorized to review")) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. You are not assigned to review this request.");
      }
      // Re-throw other runtime exceptions
      throw e;
    }
  }
}
