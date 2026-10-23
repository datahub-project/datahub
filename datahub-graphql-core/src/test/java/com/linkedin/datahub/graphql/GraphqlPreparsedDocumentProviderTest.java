package com.linkedin.datahub.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import graphql.ExecutionInput;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.language.Document;
import graphql.parser.Parser;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class GraphqlPreparsedDocumentProviderTest {

  private static final Parser PARSER = new Parser();

  private static PreparsedDocumentEntry parseAndValidate(ExecutionInput executionInput) {
    Document document = PARSER.parseDocument(executionInput.getQuery());
    return new PreparsedDocumentEntry(document);
  }

  @Test
  public void testSecondCallForSameQuerySkipsParseAndValidateFunction() {
    GraphqlPreparsedDocumentProvider provider =
        new GraphqlPreparsedDocumentProvider(new GraphqlDocumentCache(1024 * 1024));
    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("{ hello }").build();
    AtomicInteger computeCount = new AtomicInteger();

    PreparsedDocumentEntry first =
        provider
            .getDocumentAsync(
                executionInput,
                input -> {
                  computeCount.incrementAndGet();
                  return parseAndValidate(input);
                })
            .join();
    PreparsedDocumentEntry second =
        provider
            .getDocumentAsync(
                executionInput,
                input -> {
                  computeCount.incrementAndGet();
                  return parseAndValidate(input);
                })
            .join();

    assertEquals(computeCount.get(), 1);
    assertSame(first, second);
  }

  @Test
  public void testDifferentQueriesEachComputeOnce() {
    GraphqlPreparsedDocumentProvider provider =
        new GraphqlPreparsedDocumentProvider(new GraphqlDocumentCache(1024 * 1024));
    AtomicInteger computeCount = new AtomicInteger();

    provider
        .getDocumentAsync(
            ExecutionInput.newExecutionInput().query("{ hello }").build(),
            input -> {
              computeCount.incrementAndGet();
              return parseAndValidate(input);
            })
        .join();
    provider
        .getDocumentAsync(
            ExecutionInput.newExecutionInput().query("{ world }").build(),
            input -> {
              computeCount.incrementAndGet();
              return parseAndValidate(input);
            })
        .join();

    assertEquals(computeCount.get(), 2);
  }
}
