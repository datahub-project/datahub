package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.ratelimit.GraphQLOperationNameResolver.RateLimitQuery;
import java.util.List;
import org.testng.annotations.Test;

public class GraphQLOperationNameResolverTest {

  // ---- resolve(): operation/display name only ----

  @Test
  public void testResolveJsonOperationNameWins() {
    assertEquals(
        GraphQLOperationNameResolver.resolve("batchIngest", "mutation { foo }"), "batchIngest");
  }

  @Test
  public void testResolveNamedMutationFromDocument() {
    assertEquals(
        GraphQLOperationNameResolver.resolve(
            null, "mutation batchIngest { ingest(input: {}) { success } }"),
        "batchIngest");
  }

  @Test
  public void testResolveAnonymousFallback() {
    assertEquals(GraphQLOperationNameResolver.resolve(null, "{ foo }"), "graphql");
  }

  @Test
  public void testResolveMalformedFallback() {
    assertEquals(GraphQLOperationNameResolver.resolve(null, "{ foo"), "graphql");
  }

  // ---- analyze(): single parse → {queryName, identity, topLevelFields} ----

  @Test
  public void testAnalyzeJsonOperationNameWins() {
    RateLimitQuery rlq = GraphQLOperationNameResolver.analyze("batchIngest", "{ search { urn } }");
    assertEquals(rlq.identity(), "batchIngest");
    assertEquals(rlq.queryName(), "batchIngest");
  }

  @Test
  public void testAnalyzeNamedOperationFromDocument() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(null, "mutation batchIngest { ingest { success } }");
    assertEquals(rlq.identity(), "batchIngest");
    assertEquals(rlq.queryName(), "batchIngest");
  }

  @Test
  public void testAnalyzeUnnamedUsesTopLevelFieldForIdentity() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(null, "{ searchAcrossEntities { count } }");
    assertEquals(rlq.identity(), "searchAcrossEntities");
    // queryName for an unnamed op is the generic "graphql"; identity is field-derived.
    assertEquals(rlq.queryName(), "graphql");
    assertEquals(rlq.topLevelFields(), List.of("searchAcrossEntities"));
  }

  @Test
  public void testAnalyzeUnnamedSortsMultipleFieldsForIdentity() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(null, "{ user { urn } search { x } }");
    assertEquals(rlq.identity(), "search,user");
    // topLevelFields preserves source order; identity sorts.
    assertEquals(rlq.topLevelFields(), List.of("user", "search"));
  }

  @Test
  public void testAnalyzeStripsAlias() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(null, "{ s: searchAcrossEntities { x } }");
    assertEquals(rlq.identity(), "searchAcrossEntities");
    assertEquals(rlq.topLevelFields(), List.of("searchAcrossEntities"));
  }

  @Test
  public void testAnalyzeSelectsNamedOperation() {
    String doc = "query One { a { x } } query Two { searchAcrossLineage { y } }";
    assertEquals(
        GraphQLOperationNameResolver.analyze("Two", doc).topLevelFields(),
        List.of("searchAcrossLineage"));
  }

  @Test
  public void testAnalyzeNoOperationFallsBack() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(null, "fragment F on Dataset { urn }");
    assertEquals(rlq.identity(), "graphql");
    assertTrue(rlq.topLevelFields().isEmpty());
  }

  @Test
  public void testAnalyzeMalformedFallsBack() {
    RateLimitQuery rlq = GraphQLOperationNameResolver.analyze(null, "{ foo");
    assertEquals(rlq.identity(), "graphql");
    assertTrue(rlq.topLevelFields().isEmpty());
    assertTrue(GraphQLOperationNameResolver.analyze(null, null).topLevelFields().isEmpty());
  }

  // ---- Heavy-gate evasion guards: a heavy resolver hidden in a top-level fragment must still be
  // surfaced, otherwise the per-resolver gate could be bypassed with a one-line query rewrite. ----

  @Test
  public void testAnalyzeResolvesInlineFragment() {
    RateLimitQuery rlq =
        GraphQLOperationNameResolver.analyze(
            null, "{ ... on Query { searchAcrossEntities { urn } } }");
    assertEquals(rlq.topLevelFields(), List.of("searchAcrossEntities"));
    assertEquals(rlq.identity(), "searchAcrossEntities");
  }

  @Test
  public void testAnalyzeResolvesNamedFragmentSpread() {
    String doc = "query { ...F } fragment F on Query { searchAcrossEntities { urn } }";
    assertEquals(
        GraphQLOperationNameResolver.analyze(null, doc).topLevelFields(),
        List.of("searchAcrossEntities"));
  }

  @Test
  public void testAnalyzeResolvesNestedFragments() {
    String doc =
        "query { ...A } "
            + "fragment A on Query { ... on Query { ...B } } "
            + "fragment B on Query { searchAcrossEntities { urn } }";
    assertEquals(
        GraphQLOperationNameResolver.analyze(null, doc).topLevelFields(),
        List.of("searchAcrossEntities"));
  }

  @Test
  public void testAnalyzeUnknownFragmentSpreadIsEmpty() {
    // Spread referencing a fragment that isn't defined contributes nothing (and must not throw).
    assertTrue(
        GraphQLOperationNameResolver.analyze(null, "{ ...Missing }").topLevelFields().isEmpty());
  }
}
