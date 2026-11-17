package com.linkedin.datahub.graphql.concurrency;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class GraphQLConcurrencyUtilsTest {

  @Test
  public void testSequence_completesWithResultsInInputOrder() {
    CompletableFuture<String> first = new CompletableFuture<>();
    CompletableFuture<String> second = new CompletableFuture<>();

    CompletableFuture<List<String>> all = GraphQLConcurrencyUtils.sequence(List.of(first, second));

    second.complete("b");
    assertFalse(all.isDone());
    first.complete("a");
    assertEquals(all.join(), List.of("a", "b"));
  }
}
