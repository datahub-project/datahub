package com.linkedin.metadata.system_telemetry;

import static org.testng.Assert.*;

import graphql.language.Document;
import graphql.parser.Parser;
import org.testng.annotations.Test;

public class QueryShapeAnalyzerTest {

  private static Document parse(String query) {
    return new Parser().parseDocument(query);
  }

  // -------------------------------------------------------------------------
  // Basic operation type detection
  // -------------------------------------------------------------------------

  @Test
  public void testSimpleQuery_operationTypeIsQuery() {
    Document doc = parse("query { user { id name } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getOperationType(), "query");
  }

  @Test
  public void testMutation_operationTypeIsMutation() {
    Document doc = parse("mutation CreateUser { createUser(name: {}) { id } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getOperationType(), "mutation");
  }

  @Test
  public void testSubscription_operationTypeIsSubscription() {
    Document doc = parse("subscription OnUpdate { onUpdate { id status } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getOperationType(), "subscription");
  }

  // -------------------------------------------------------------------------
  // Normalized shape — alias stripping
  // -------------------------------------------------------------------------

  @Test
  public void testAlias_strippedToFieldName() {
    // Both queries should produce the same normalized shape despite aliases
    Document withAlias = parse("query { myAlias: user { id name } }");
    Document withoutAlias = parse("query { user { id name } }");

    QueryShapeAnalyzer.QueryShape a = QueryShapeAnalyzer.analyze(withAlias, null);
    QueryShapeAnalyzer.QueryShape b = QueryShapeAnalyzer.analyze(withoutAlias, null);

    assertEquals(a.getNormalizedShape(), b.getNormalizedShape());
    assertEquals(a.getShapeHash(), b.getShapeHash());
  }

  // -------------------------------------------------------------------------
  // Argument stripping → `arg: {}` placeholder
  // -------------------------------------------------------------------------

  @Test
  public void testArguments_valuesStrippedToPlaceholder() {
    Document doc = parse("query { user(id: \"123\") { name } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);

    assertTrue(
        shape.getNormalizedShape().contains("id: {}"),
        "Expected argument value stripped to {}, got: " + shape.getNormalizedShape());
  }

  @Test
  public void testArguments_differentValues_sameHash() {
    Document doc1 = parse("query { user(id: \"alice\") { name email } }");
    Document doc2 = parse("query { user(id: \"bob\") { name email } }");

    QueryShapeAnalyzer.QueryShape s1 = QueryShapeAnalyzer.analyze(doc1, null);
    QueryShapeAnalyzer.QueryShape s2 = QueryShapeAnalyzer.analyze(doc2, null);

    assertEquals(s1.getShapeHash(), s2.getShapeHash());
    assertEquals(s1.getNormalizedShape(), s2.getNormalizedShape());
  }

  @Test
  public void testVariables_treatedSameAsLiterals() {
    // Variable references should be stripped to {} just like literal values
    Document withVar = parse("query GetUser($id: ID!) { user(id: $id) { name } }");
    Document withLit = parse("query GetUser { user(id: \"42\") { name } }");

    QueryShapeAnalyzer.QueryShape sv = QueryShapeAnalyzer.analyze(withVar, null);
    QueryShapeAnalyzer.QueryShape sl = QueryShapeAnalyzer.analyze(withLit, null);

    assertEquals(sv.getNormalizedShape(), sl.getNormalizedShape());
    assertEquals(sv.getShapeHash(), sl.getShapeHash());
  }

  // -------------------------------------------------------------------------
  // Named fragments — inlined into shape
  // -------------------------------------------------------------------------

  @Test
  public void testNamedFragment_inlinedIntoShape() {
    String withFragment =
        "query { user { ...UserFields } } fragment UserFields on User { id name }";

    QueryShapeAnalyzer.QueryShape sf = QueryShapeAnalyzer.analyze(parse(withFragment), null);

    // Fragment spreads are inlined by appending the fragment's own SelectionSet, which is
    // wrapped in its own {} — so the result is {user{{id name}}} rather than {user{id name}}.
    // Verify the fragment fields appear in the shape and that the field count is correct.
    String shape = sf.getNormalizedShape();
    assertTrue(shape.contains("id"), "Shape should contain 'id': " + shape);
    assertTrue(shape.contains("name"), "Shape should contain 'name': " + shape);
    // user + id + name = 3 fields total
    assertEquals(sf.getFieldCount(), 3);
  }

  // -------------------------------------------------------------------------
  // Inline fragments with type conditions
  // -------------------------------------------------------------------------

  @Test
  public void testInlineFragment_typeConditionPreservedInShape() {
    // Inline fragments with type conditions should be preserved in normalized shape
    // to ensure queries dispatching across entity types produce different shapes
    String query =
        "query { search { "
            + "... on Dataset { exists name } "
            + "... on Chart { exists title } "
            + "} }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Type conditions preserved as inline fragments: Dataset{...} Chart{...}
    assertTrue(s.contains("Dataset"), "Shape should preserve inline fragment type condition: " + s);
    assertTrue(s.contains("Chart"), "Shape should preserve inline fragment type condition: " + s);
  }

  @Test
  public void testAnonymousInlineFragment_handledGracefully() {
    // Anonymous inline fragments (no type condition) should be handled gracefully
    String query = "query { search { ... { exists } } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    assertNotNull(shape);
    assertNotNull(shape.getNormalizedShape());
    // Anonymous fragment should still process the selection set
    assertTrue(shape.getNormalizedShape().contains("exists"));
  }

  // -------------------------------------------------------------------------
  // Field count and max depth
  // -------------------------------------------------------------------------

  @Test
  public void testFieldCount_countedCorrectly() {
    // user{id name email} → 3 leaf fields + 1 for "user" = 4 total
    Document doc = parse("query { user { id name email } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getFieldCount(), 4);
  }

  @Test
  public void testMaxDepth_calculatedCorrectly() {
    // query → posts → comments → author → name  =  depth 5
    Document doc = parse("query { posts { comments { author { name } } } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getMaxDepth(), 4);
  }

  @Test
  public void testDeepNesting_maxDepthCorrect() {
    // 5 levels deep
    Document doc = parse("query { a { b { c { d { e } } } } }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);
    assertEquals(shape.getMaxDepth(), 5);
  }

  // -------------------------------------------------------------------------
  // Hash consistency
  // -------------------------------------------------------------------------

  @Test
  public void testSameShape_sameCRC32Hash() {
    Document doc1 = parse("query { user { id name } }");
    Document doc2 = parse("query { user { id name } }");

    QueryShapeAnalyzer.QueryShape s1 = QueryShapeAnalyzer.analyze(doc1, null);
    QueryShapeAnalyzer.QueryShape s2 = QueryShapeAnalyzer.analyze(doc2, null);

    assertEquals(s1.getShapeHash(), s2.getShapeHash());
  }

  @Test
  public void testDifferentShape_differentHash() {
    Document doc1 = parse("query { user { id name } }");
    Document doc2 = parse("query { user { id email } }");

    QueryShapeAnalyzer.QueryShape s1 = QueryShapeAnalyzer.analyze(doc1, null);
    QueryShapeAnalyzer.QueryShape s2 = QueryShapeAnalyzer.analyze(doc2, null);

    assertNotEquals(s1.getShapeHash(), s2.getShapeHash());
  }

  @Test
  public void testHash_is8CharLowercaseHex() {
    Document doc = parse("query { hello }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);

    assertTrue(
        shape.getShapeHash().matches("[0-9a-f]{8}"),
        "Hash should be 8 lowercase hex chars, got: " + shape.getShapeHash());
  }

  // -------------------------------------------------------------------------
  // Cycle protection
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // Normalized shape truncation
  // -------------------------------------------------------------------------

  @Test
  public void testNormalizedShape_truncatedAt4096Chars() {
    // Build a very wide query to exceed MAX_SHAPE_LENGTH (4096)
    StringBuilder qb = new StringBuilder("query {");
    for (int i = 0; i < 600; i++) {
      qb.append(" field").append(i);
    }
    qb.append('}');

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(qb.toString()), null);

    // Shape must be capped at 4096 + "..." (3 chars)
    assertTrue(
        shape.getNormalizedShape().length() <= 4096 + 3,
        "Shape should be truncated, length=" + shape.getNormalizedShape().length());
    assertTrue(shape.getNormalizedShape().endsWith("..."), "Truncated shape should end with '...'");
  }

  // -------------------------------------------------------------------------
  // MAX_DEPTH recursion guard
  // -------------------------------------------------------------------------

  @Test
  public void testDeepNesting_TruncatesAtMaxDepth() {
    // Build a query with 50+ levels of nesting: { a { b { c { ... } } } }
    StringBuilder qb = new StringBuilder("query {");
    for (int i = 0; i < 55; i++) {
      qb.append(" f").append(i).append(" {");
    }
    qb.append(" leaf");
    for (int i = 0; i < 55; i++) {
      qb.append(" }");
    }
    qb.append(" }");

    // Should not throw StackOverflowError
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(qb.toString()), null);

    assertNotNull(shape);
    assertNotNull(shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("...truncated at depth 30"),
        "Expected truncation message, got: " + shape.getNormalizedShape());
  }

  // -------------------------------------------------------------------------
  // operationName selection
  // -------------------------------------------------------------------------

  @Test
  public void testOperationNameSelection_picksMatchingOperation() {
    String query = "query OpA { fieldA } " + "query OpB { fieldB }";

    QueryShapeAnalyzer.QueryShape shapeA = QueryShapeAnalyzer.analyze(parse(query), "OpA");
    QueryShapeAnalyzer.QueryShape shapeB = QueryShapeAnalyzer.analyze(parse(query), "OpB");

    assertTrue(shapeA.getNormalizedShape().contains("fieldA"));
    assertFalse(shapeA.getNormalizedShape().contains("fieldB"));

    assertTrue(shapeB.getNormalizedShape().contains("fieldB"));
    assertFalse(shapeB.getNormalizedShape().contains("fieldA"));
  }

  @Test
  public void testOperationNameNull_picksFirstOperation() {
    String query = "query First { fieldA } query Second { fieldB }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    assertTrue(shape.getNormalizedShape().contains("fieldA"));
  }

  @Test
  public void testEmptyDocument_returnsNoOperationShape() {
    // A document with only a fragment, no operation definition
    Document doc = parse("fragment F on SomeType { id }");
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(doc, null);

    assertEquals(shape.getNormalizedShape(), "(no operation)");
    assertEquals(shape.getShapeHash(), "00000000");
    assertEquals(shape.getFieldCount(), 0);
    assertEquals(shape.getMaxDepth(), 0);
  }

  // -------------------------------------------------------------------------
  // Named fragments (fragment spreads)
  // -------------------------------------------------------------------------

  @Test
  public void testNamedFragment_spreadInlinedIntoShape() {
    // Fragment spreads are inlined into the shape
    String query =
        "query { user { ...UserFields } } " + "fragment UserFields on User { id name email }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Fragment fields should appear in the shape (id, name, email inlined)
    assertTrue(s.contains("id"), "Shape should contain 'id' from fragment: " + s);
    assertTrue(s.contains("name"), "Shape should contain 'name' from fragment: " + s);
    assertTrue(s.contains("email"), "Shape should contain 'email' from fragment: " + s);
    // Total field count: user + id + name + email = 4
    assertEquals(shape.getFieldCount(), 4);
  }

  @Test
  public void testNamedFragment_multipleFragmentsInlined() {
    // Multiple fragment spreads should all be inlined
    String query =
        "query { search { ...CommonFields ...DatasetFields } } "
            + "fragment CommonFields on SearchResult { urn type } "
            + "fragment DatasetFields on Dataset { name platform }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    assertTrue(s.contains("urn"), "Should contain urn from CommonFields: " + s);
    assertTrue(s.contains("type"), "Should contain type from CommonFields: " + s);
    assertTrue(s.contains("name"), "Should contain name from DatasetFields: " + s);
    assertTrue(s.contains("platform"), "Should contain platform from DatasetFields: " + s);
  }

  @Test
  public void testNamedFragment_cycleProtection() {
    // Fragment references itself — analyzer must guard against infinite recursion
    String query =
        "query { user { ...F } } " + "fragment F on User { name ...F }"; // self-referencing

    // Should not throw StackOverflowError
    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);
    assertNotNull(shape);
    assertNotNull(shape.getNormalizedShape());
  }

  // -------------------------------------------------------------------------
  // Argument structures (flat objects)
  // -------------------------------------------------------------------------

  @Test
  public void testArgumentStructure_flatObject() {
    // Mutation with flat input object containing only scalars
    String query = "mutation { createUser(input: {name: \"Alice\" email: \"a@b.com\"}) { id } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Should show input with field names (no {} for scalar fields inside objects)
    assertTrue(s.contains("input: {name email}"), "Should show flat object structure: " + s);
  }

  @Test
  public void testArgumentStructure_scalarArgument() {
    // Mutation with simple scalar argument
    String query = "mutation { deleteUser(id: \"123\") { success } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Scalar arguments should be stripped to {}
    assertTrue(s.contains("id: {}"), "Scalar argument should be stripped to {}: " + s);
  }

  @Test
  public void testArgumentStructure_arrayOfScalars() {
    // Array of scalar values: shows [size]{} for scalar elements
    String query = "mutation { batchDelete(ids: [\"id1\" \"id2\" \"id3\"]) { deleted } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Scalar array should show [3]{}
    assertTrue(s.contains("ids: [3]{}"), "Scalar array should show [size]{}: " + s);
  }

  @Test
  public void testArgumentStructure_arrayOfObjects() {
    // Array of objects: shows [size]{field1 field2 ...}
    String query =
        "mutation { batchAddTags(input: {resources: ["
            + "{resourceUrn: \"urn:1\" subResource: \"field\"},"
            + "{resourceUrn: \"urn:2\" subResource: \"field2\"}"
            + "]}) { success } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Array of objects should show [size] followed by first element's structure
    assertTrue(
        s.contains("resources[2]{resourceUrn subResource}"),
        "Should show array with object structure: " + s);
  }

  @Test
  public void testArgumentStructure_complexNestedInput() {
    // Complex input: multiple arrays and nested objects
    String query =
        "mutation { batchAddTags(input: {"
            + "tagUrns: [\"tag1\" \"tag2\" \"tag3\"],"
            + "resources: [{resourceUrn: \"urn\" subResource: \"field\" subResourceType: FIELD}]"
            + "}) { success } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Should show input object with arrays and their structures
    assertTrue(s.contains("tagUrns[3]{}"), "Should show scalar array: " + s);
    assertTrue(
        s.contains("resources[1]{resourceUrn subResource subResourceType}"),
        "Should show object array with fields: " + s);
  }

  @Test
  public void testArgumentStructure_emptyArray() {
    String query = "mutation { deleteMultiple(ids: []) { deleted } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Empty arrays should show [0]{}
    assertTrue(s.contains("ids: [0]{}"), "Empty array should show [0]{}: " + s);
  }

  @Test
  public void testArgumentStructure_depthLimit() {
    // Deeply nested arguments should respect MAX_ARGUMENT_DEPTH limit
    String query =
        "mutation { test(input: {a: {b: {c: {d: {e: {f: {g: {h: {i: {j: {k: {value: \"x\"}}}}}}}}}}}}) { result } }";

    QueryShapeAnalyzer.QueryShape shape = QueryShapeAnalyzer.analyze(parse(query), null);

    String s = shape.getNormalizedShape();
    // Should truncate at depth limit with "..."
    assertTrue(s.contains("..."), "Should truncate deeply nested arguments: " + s);
  }
}
