package com.linkedin.datahub.graphql;

import graphql.ExecutionInput;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.execution.preparsed.PreparsedDocumentProvider;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Adapts {@link GraphqlDocumentCache} to graphql-java's {@link PreparsedDocumentProvider} SPI.
 *
 * <p>The cache key is the query text only. That's correct today because DataHub never sets a
 * per-request {@code Locale} (which can localize validation error messages) or the internal
 * validation-rule-skipping predicate hint on {@code ExecutionInput} -- if either is ever wired up
 * per-request, it would need to become part of the key too, or a cached entry could be served
 * across two requests that should have validated differently.
 */
final class GraphqlPreparsedDocumentProvider implements PreparsedDocumentProvider {

  private final GraphqlDocumentCache cache;

  GraphqlPreparsedDocumentProvider(GraphqlDocumentCache cache) {
    this.cache = cache;
  }

  @Override
  public CompletableFuture<PreparsedDocumentEntry> getDocumentAsync(
      ExecutionInput executionInput,
      Function<ExecutionInput, PreparsedDocumentEntry> parseAndValidateFunction) {
    PreparsedDocumentEntry entry =
        cache.getOrCompute(
            executionInput.getQuery(), () -> parseAndValidateFunction.apply(executionInput));
    return CompletableFuture.completedFuture(entry);
  }
}
