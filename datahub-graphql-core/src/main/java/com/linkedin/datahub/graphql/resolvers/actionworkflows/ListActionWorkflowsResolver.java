package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeUrnMapper;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.ActionWorkflowService.ActionWorkflowListResult;
import com.linkedin.metadata.service.ActionWorkflowService.ActionWorkflowResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListActionWorkflowsResolver
    implements DataFetcher<CompletableFuture<ListActionWorkflowResult>> {

  private final ActionWorkflowService _actionWorkflowService;

  @Override
  public CompletableFuture<ListActionWorkflowResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ListActionWorkflowsInput input =
        bindArgument(environment.getArgument("input"), ListActionWorkflowsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return listActionWorkflows(input, context);
          } catch (Exception e) {
            log.error("Failed to list action workflows", e);
            throw new RuntimeException("Failed to list action workflows", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private ListActionWorkflowResult listActionWorkflows(
      @Nonnull final ListActionWorkflowsInput input, @Nonnull final QueryContext context)
      throws Exception {

    // Convert GraphQL input to service parameters
    final Integer start = input.getStart();
    final Integer count = input.getCount();
    final ActionWorkflowCategory category =
        input.getCategory() != null
            ? ActionWorkflowCategory.valueOf(input.getCategory().toString())
            : null;
    final String customCategory = input.getCustomCategory();
    final String entrypointType =
        input.getEntrypointType() != null ? input.getEntrypointType().toString() : null;
    final String entityType =
        input.getEntityType() != null
            ? EntityTypeUrnMapper.getEntityTypeUrn(EntityTypeMapper.getName(input.getEntityType()))
            : null;

    // Call the service method
    final ActionWorkflowListResult serviceResult =
        _actionWorkflowService.listActionWorkflows(
            context.getOperationContext(),
            start,
            count,
            category,
            customCategory,
            entrypointType,
            entityType);

    // Convert service results to GraphQL objects
    final List<ActionWorkflow> graphqlWorkflows =
        serviceResult.getWorkflows().stream()
            .map(this::mapToGraphQLWorkflow)
            .collect(Collectors.toList());

    // Create and return GraphQL result
    final ListActionWorkflowResult result = new ListActionWorkflowResult();
    result.setStart(serviceResult.getStart());
    result.setCount(serviceResult.getCount());
    result.setTotal(serviceResult.getTotal());
    result.setWorkflows(graphqlWorkflows);

    return result;
  }

  private ActionWorkflow mapToGraphQLWorkflow(ActionWorkflowResult workflowResult) {
    return ActionWorkflowMapper.map(workflowResult.getUrn(), workflowResult.getInfo());
  }
}
