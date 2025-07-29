package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateActionWorkflowFormRequestResolver
    implements DataFetcher<CompletableFuture<String>> {

  private final ActionWorkflowService _actionWorkflowService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateActionWorkflowFormRequestInput input =
        bindArgument(environment.getArgument("input"), CreateActionWorkflowFormRequestInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return createActionWorkflowRequest(input, context);
          } catch (Exception e) {
            log.error("Failed to create action workflow request", e);
            throw new RuntimeException("Failed to create action workflow request", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private String createActionWorkflowRequest(
      @Nonnull final CreateActionWorkflowFormRequestInput input,
      @Nonnull final QueryContext context)
      throws Exception {

    // Convert workflow ID to URN
    final Urn workflowUrn = UrnUtils.getUrn(input.getWorkflowUrn());

    // Convert entity URN if provided
    final Urn entityUrn =
        input.getEntityUrn() != null ? UrnUtils.getUrn(input.getEntityUrn()) : null;

    // Map field values
    final java.util.List<com.linkedin.actionworkflow.ActionWorkflowFormRequestField> fields =
        input.getFields().stream()
            .map(this::mapWorkflowFormRequestField)
            .collect(Collectors.toList());

    // Get expiration time from access field if available
    Long expiresAt = null;
    if (input.getAccess() != null && input.getAccess().getExpiresAt() != null) {
      expiresAt = input.getAccess().getExpiresAt();
    }

    // Create the action request with workflow request using the service
    final Urn actionRequestUrn =
        _actionWorkflowService.createActionWorkflowFormRequest(
            workflowUrn,
            entityUrn,
            input.getDescription(),
            fields,
            expiresAt,
            context.getOperationContext());

    // Return the URN as a string
    return actionRequestUrn.toString();
  }

  private com.linkedin.actionworkflow.ActionWorkflowFormRequestField mapWorkflowFormRequestField(
      @Nonnull final ActionWorkflowFormRequestFieldInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowFormRequestField field =
        new com.linkedin.actionworkflow.ActionWorkflowFormRequestField();
    field.setId(input.getId());

    // Create PrimitivePropertyValueArray from the input values
    com.linkedin.structured.PrimitivePropertyValueArray values =
        new com.linkedin.structured.PrimitivePropertyValueArray();
    for (PropertyValueInput valueInput : input.getValues()) {
      if (valueInput.getStringValue() != null) {
        values.add(
            com.linkedin.structured.PrimitivePropertyValue.create(valueInput.getStringValue()));
      } else if (valueInput.getNumberValue() != null) {
        values.add(
            com.linkedin.structured.PrimitivePropertyValue.create(
                valueInput.getNumberValue().doubleValue()));
      }
    }
    field.setValues(values);
    return field;
  }
}
