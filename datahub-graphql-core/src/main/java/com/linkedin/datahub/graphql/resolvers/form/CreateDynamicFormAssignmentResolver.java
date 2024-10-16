package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CreateDynamicFormAssignmentInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class CreateDynamicFormAssignmentResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final FormService _formService;

  public CreateDynamicFormAssignmentResolver(@Nonnull final FormService formService) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final CreateDynamicFormAssignmentInput input =
        bindArgument(environment.getArgument("input"), CreateDynamicFormAssignmentInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getFormUrn());
    final DynamicFormAssignment formAssignment = FormUtils.mapDynamicFormAssignment(input);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _formService.createDynamicFormAssignment(
                context.getOperationContext(), formAssignment, formUrn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
