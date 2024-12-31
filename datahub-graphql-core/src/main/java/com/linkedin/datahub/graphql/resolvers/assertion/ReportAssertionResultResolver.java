package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AssertionResultInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.metadata.service.AssertionService;
import graphql.execution.DataFetcherExceptionHandler;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReportAssertionResultResolver implements DataFetcher<CompletableFuture<Boolean>> {

  public static final String ERROR_MESSAGE_KEY = "message";
  private final AssertionService _assertionService;

  public ReportAssertionResultResolver(AssertionService assertionService) {
    _assertionService = assertionService;
  }

  /**
   * This is called by the graphql engine to fetch the value. The {@link DataFetchingEnvironment} is
   * a composite context object that tells you all you need to know about how to fetch a data value
   * in graphql type terms.
   *
   * @param environment this is the data fetching environment which contains all the context you
   *     need to fetch a value
   * @return a value of type T. May be wrapped in a {@link DataFetcherResult}
   * @throws Exception to relieve the implementations from having to wrap checked exceptions. Any
   *     exception thrown from a {@code DataFetcher} will eventually be handled by the registered
   *     {@link DataFetcherExceptionHandler} and the related field will have a value of {@code null}
   *     in the result.
   */
  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn assertionUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final AssertionResultInput input =
        bindArgument(environment.getArgument("result"), AssertionResultInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          final Urn asserteeUrn =
              _assertionService.getEntityUrnForAssertion(
                  context.getOperationContext(), assertionUrn);
          if (asserteeUrn == null) {
            throw new RuntimeException(
                String.format(
                    "Failed to report Assertion Run Event. Assertion with urn %s does not exist or is not associated with any entity.",
                    assertionUrn));
          }

          // Check whether the current user is allowed to update the assertion.
          if (AssertionUtils.isAuthorizedToEditAssertionFromAssertee(context, asserteeUrn)) {
            AssertionResult assertionResult = mapAssertionResult(input);
            _assertionService.addAssertionRunEvent(
                context.getOperationContext(),
                assertionUrn,
                asserteeUrn,
                input.getTimestampMillis() != null
                    ? input.getTimestampMillis()
                    : System.currentTimeMillis(),
                assertionResult);
            return true;
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private static StringMap mapContextParameters(List<StringMapEntryInput> input) {

    if (input == null || input.isEmpty()) {
      return null;
    }
    StringMap entries = new StringMap();
    input.forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
    return entries;
  }

  private AssertionResult mapAssertionResult(AssertionResultInput input) {
    AssertionResult assertionResult = new AssertionResult();
    assertionResult.setType(AssertionResultType.valueOf(input.getType().toString()));
    assertionResult.setExternalUrl(input.getExternalUrl(), SetMode.IGNORE_NULL);
    if (assertionResult.getType() == AssertionResultType.ERROR && input.getError() != null) {
      assertionResult.setError(mapAssertionResultError(input));
    }
    if (input.getProperties() != null) {
      assertionResult.setNativeResults(mapContextParameters(input.getProperties()));
    }
    return assertionResult;
  }

  private static AssertionResultError mapAssertionResultError(AssertionResultInput input) {
    AssertionResultError error = new AssertionResultError();
    error.setType(AssertionResultErrorType.valueOf(input.getError().getType().toString()));
    StringMap errorProperties = new StringMap();
    errorProperties.put(ERROR_MESSAGE_KEY, input.getError().getMessage());
    error.setProperties(errorProperties);
    return error;
  }
}
