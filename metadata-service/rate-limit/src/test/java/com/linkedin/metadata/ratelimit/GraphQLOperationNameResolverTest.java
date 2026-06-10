package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class GraphQLOperationNameResolverTest {

  @Test
  public void testJsonOperationNameWins() {
    assertEquals(
        GraphQLOperationNameResolver.resolve("batchIngest", "mutation { foo }"), "batchIngest");
  }

  @Test
  public void testNamedMutationFromDocument() {
    String document = "mutation batchIngest { ingest(input: {}) { success } }";
    assertEquals(GraphQLOperationNameResolver.resolve(null, document), "batchIngest");
  }

  @Test
  public void testAnonymousFallback() {
    assertEquals(GraphQLOperationNameResolver.resolve(null, "{ foo }"), "graphql");
  }

  @Test
  public void testMalformedQueryFallback() {
    assertEquals(GraphQLOperationNameResolver.resolve(null, "{ foo"), "graphql");
  }
}
