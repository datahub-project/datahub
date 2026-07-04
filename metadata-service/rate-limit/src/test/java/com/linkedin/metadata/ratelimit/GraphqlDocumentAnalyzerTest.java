package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import graphql.language.OperationDefinition;
import java.util.List;
import org.testng.annotations.Test;

public class GraphqlDocumentAnalyzerTest {

  @Test
  public void testJsonOperationNameWinsWithoutParse() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze("batchIngest", "mutation { foo }", name -> true);
    assertEquals(metadata.resolvedOperationName(), "batchIngest");
    assertFalse(metadata.isParsed());
    assertTrue(metadata.getOperations().isEmpty());
  }

  @Test
  public void testNamedMutationFromDocument() {
    String document = "mutation batchIngest { ingest(input: {}) { success } }";
    GraphqlDocumentMetadata metadata = GraphqlDocumentAnalyzer.analyze(null, document);
    assertEquals(metadata.resolvedOperationName(), "batchIngest");
    assertTrue(metadata.isParsed());
    assertEquals(metadata.getOperations().size(), 1);
    assertEquals(metadata.getOperations().get(0).getName(), "batchIngest");
    assertEquals(metadata.getOperations().get(0).getKind(), OperationDefinition.Operation.MUTATION);
  }

  @Test
  public void testAnonymousFallback() {
    GraphqlDocumentMetadata metadata = GraphqlDocumentAnalyzer.analyze(null, "{ foo }");
    assertEquals(
        metadata.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertTrue(metadata.isParsed());
    assertEquals(metadata.getOperations().get(0).getRootFields(), List.of("foo"));
  }

  @Test
  public void testMalformedQueryFallback() {
    GraphqlDocumentMetadata metadata = GraphqlDocumentAnalyzer.analyze(null, "{ foo");
    assertEquals(
        metadata.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertFalse(metadata.isParsed());
    assertTrue(metadata.getOperations().isEmpty());
  }

  @Test
  public void testSingleRootField() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            null, "query { dataset(urn: \"urn:li:dataset:(a,b,c)\") { urn } }");
    assertEquals(metadata.allRootFields(), List.of("dataset"));
  }

  @Test
  public void testMultipleRootFieldsInOneOperation() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            null, "query { me { corpUser { urn } } search(input: {}) { total } }");
    assertEquals(metadata.allRootFields(), List.of("me", "search"));
  }

  @Test
  public void testNamedOperationDocument() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            null, "query getDataset { dataset(urn: \"urn:li:dataset:(a,b,c)\") { urn } }");
    assertEquals(metadata.resolvedOperationName(), "getDataset");
    assertEquals(metadata.allRootFields(), List.of("dataset"));
  }

  @Test
  public void testAnonymousShorthand() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(null, "{ search(input: {}) { total } }");
    assertEquals(metadata.allRootFields(), List.of("search"));
  }

  @Test
  public void testNullOrBlankQueryReturnsAnonymous() {
    GraphqlDocumentMetadata nullQuery = GraphqlDocumentAnalyzer.analyze(null, null);
    assertEquals(
        nullQuery.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertFalse(nullQuery.isParsed());

    GraphqlDocumentMetadata blankQuery = GraphqlDocumentAnalyzer.analyze(null, "  ");
    assertEquals(
        blankQuery.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertFalse(blankQuery.isParsed());
  }

  @Test
  public void testMultipleOperationsCollectsAllRootFields() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            null,
            """
            query first { me { corpUser { urn } } }
            query second { search(input: {}) { total } }
            """);
    assertEquals(metadata.getOperations().size(), 2);
    assertEquals(metadata.allRootFields(), List.of("me", "search"));
  }

  @Test
  public void testSelectedOperationRespectsHttpOperationName() {
    String document =
        """
        query first { me { corpUser { urn } } }
        query second { search(input: {}) { total } }
        """;
    GraphqlDocumentMetadata metadata = GraphqlDocumentAnalyzer.analyze("second", document);
    assertEquals(metadata.selectedOperation().getName(), "second");
    assertEquals(metadata.selectedOperation().getRootFields(), List.of("search"));
    assertEquals(metadata.allRootFields(), List.of("search"));
  }

  @Test
  public void testSkipParseWhenNamedPredicateTrue() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze("search", "query { dataset { urn } }", name -> true);
    assertEquals(metadata.resolvedOperationName(), "search");
    assertFalse(metadata.isParsed());
  }

  @Test
  public void testSkipParseWhenNamedPredicateFalseStillParses() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze("getDataset", "query { dataset { urn } }", name -> false);
    assertEquals(metadata.resolvedOperationName(), "getDataset");
    assertTrue(metadata.isParsed());
    assertEquals(metadata.allRootFields(), List.of("dataset"));
  }

  @Test
  public void testUnparsedAllRootFieldsReturnsEmpty() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze("search", "query { dataset { urn } }", name -> true);
    assertFalse(metadata.isParsed());
    assertTrue(metadata.allRootFields().isEmpty());
  }

  @Test
  public void testPrefixOperationKindMutation() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentMetadata.unparsed(null, "graphql", "mutation { deleteEntity { urn } }");
    assertEquals(metadata.prefixOperationKind(), OperationDefinition.Operation.MUTATION);
  }

  @Test
  public void testPrefixOperationKindSubscriptionAndQueryDefault() {
    assertEquals(
        GraphqlDocumentAnalyzer.detectPrefixOperationKind("subscription { tick }"),
        OperationDefinition.Operation.SUBSCRIPTION);
    assertEquals(
        GraphqlDocumentAnalyzer.detectPrefixOperationKind("  query { me { urn } }"),
        OperationDefinition.Operation.QUERY);
    assertEquals(
        GraphqlDocumentAnalyzer.detectPrefixOperationKind("{ me { urn } }"),
        OperationDefinition.Operation.QUERY);
    assertEquals(
        GraphqlDocumentAnalyzer.detectPrefixOperationKind(null),
        OperationDefinition.Operation.QUERY);
    assertEquals(
        GraphqlDocumentAnalyzer.detectPrefixOperationKind("   "),
        OperationDefinition.Operation.QUERY);
  }

  @Test
  public void testHttpOperationNameWithEmptyParseFallsBackToUnparsed() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze("batchIngest", "{ not valid", null);
    assertEquals(metadata.resolvedOperationName(), "batchIngest");
    assertFalse(metadata.isParsed());
    assertEquals(metadata.getHttpOperationName(), "batchIngest");
  }

  @Test
  public void testSelectedOperationEmptyUsesPrefixKind() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentMetadata.unparsed(null, "graphql", "mutation { x }");
    GraphqlOperationMetadata selected = metadata.selectedOperation();
    assertEquals(selected.getName(), null);
    assertEquals(selected.getKind(), OperationDefinition.Operation.MUTATION);
    assertTrue(selected.getRootFields().isEmpty());
  }

  @Test
  public void testSelectedOperationFallsBackToFirstNamedThenFirst() {
    String document =
        """
        query { anonymousRoot { urn } }
        query namedOp { me { corpUser { urn } } }
        """;
    GraphqlDocumentMetadata withMissingHttpName =
        GraphqlDocumentAnalyzer.analyze("missing", document);
    assertEquals(withMissingHttpName.selectedOperation().getName(), "namedOp");

    GraphqlDocumentMetadata noHttpName = GraphqlDocumentAnalyzer.analyze(null, document);
    assertEquals(noHttpName.selectedOperation().getName(), "namedOp");
  }

  @Test
  public void testAnonymousFactoryAndEquality() {
    GraphqlDocumentMetadata a = GraphqlDocumentMetadata.anonymous(null, "{ me { urn } }");
    GraphqlDocumentMetadata b = GraphqlDocumentMetadata.anonymous(null, "{ me { urn } }");
    GraphqlDocumentMetadata c = GraphqlDocumentMetadata.anonymous("httpName", "{ me { urn } }");

    assertEquals(a.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertFalse(a.isParsed());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertFalse(a.equals(c));
    assertFalse(a.equals(null));
    assertFalse(a.equals("not-metadata"));
  }

  @Test
  public void testParsedMetadataEqualityIncludesOperations() {
    GraphqlDocumentMetadata first =
        GraphqlDocumentAnalyzer.analyze(null, "query getMe { me { corpUser { urn } } }");
    GraphqlDocumentMetadata second =
        GraphqlDocumentAnalyzer.analyze(null, "query getMe { me { corpUser { urn } } }");
    GraphqlDocumentMetadata different =
        GraphqlDocumentAnalyzer.analyze(null, "query getMe { search(input: {}) { total } }");

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertFalse(first.equals(different));
    assertEquals(first.getQueryDocument(), "query getMe { me { corpUser { urn } } }");
  }

  @Test
  public void testFragmentOnlyDocumentIsAnonymousUnparsed() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(null, "fragment F on Dataset { urn }");
    assertEquals(
        metadata.resolvedOperationName(), GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);
    assertFalse(metadata.isParsed());
  }
}
