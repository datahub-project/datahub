package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.CreateDatasetAssertionInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateDatasetAssertionResolver implements DataFetcher<CompletableFuture<Assertion>> {

  private final AssertionService _assertionService;

  public CreateDatasetAssertionResolver(@Nonnull final AssertionService assertionService) {
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
  }

  @Override
  public CompletableFuture<Assertion> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateDatasetAssertionInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), CreateDatasetAssertionInput.class);
    final Urn asserteeUrn = UrnUtils.getUrn(input.getDatasetUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {

            // First create the new assertion.
            final Urn assertionUrn =
                _assertionService.createDatasetAssertion(
                    context.getOperationContext(),
                    asserteeUrn,
                    DatasetAssertionScope.valueOf(input.getScope().toString()),
                    input.getFieldUrns() != null
                        ? input.getFieldUrns().stream()
                            .map(UrnUtils::getUrn)
                            .collect(Collectors.toList())
                        : null,
                    input.getAggregation() != null
                        ? AssertionStdAggregation.valueOf(input.getAggregation().toString())
                        : null,
                    AssertionStdOperator.valueOf(input.getOperator().toString()),
                    input.getParameters() != null
                        ? AssertionUtils.createDatasetAssertionParameters(input.getParameters())
                        : null,
                    input.getActions() != null
                        ? AssertionUtils.createAssertionActions(input.getActions())
                        : null);

            // Then, return the new assertion
            return AssertionMapper.map(
                context,
                _assertionService.getAssertionEntityResponse(
                    context.getOperationContext(), assertionUrn));
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }
}
