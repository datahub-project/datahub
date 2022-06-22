package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.datahub.graphql.generated.TestDefinitionInput;
import com.linkedin.datahub.graphql.generated.TestValidationResult;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.ValidationResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/**
 * GraphQL Resolver used for validating JSON test definition
 */
@Slf4j
public class ValidateTestResolver implements DataFetcher<CompletableFuture<TestValidationResult>> {

  private final TestEngine _testEngine;

  public ValidateTestResolver(final TestEngine testEngine) {
    _testEngine = testEngine;
  }

  @Override
  public CompletableFuture<TestValidationResult> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final TestDefinitionInput testDefinitionInput =
          bindArgument(environment.getArgument("input"), TestDefinitionInput.class);

      ValidationResult validationResult = _testEngine.validateJson(testDefinitionInput.getJson());
      TestValidationResult graphQLResult = new TestValidationResult();
      graphQLResult.setIsValid(validationResult.isValid());
      graphQLResult.setMessages(validationResult.getMessages());
      return graphQLResult;
    });
  }
}