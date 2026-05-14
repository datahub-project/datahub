package com.linkedin.metadata.system_telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import graphql.ExecutionResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ResponseShapeAnalyzerTest {

  // -------------------------------------------------------------------------
  // Null data
  // -------------------------------------------------------------------------

  @Test
  public void testNullData_gracefullyHandled() {
    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(null);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    assertNotNull(shape);
    assertEquals(shape.getNormalizedShape(), "(null)");
    assertEquals(shape.getFieldCount(), 0);
  }

  // -------------------------------------------------------------------------
  // Flat response
  // -------------------------------------------------------------------------

  @Test
  public void testFlatResponse_correctFieldCount() {
    // { "id": "1", "name": "Alice", "email": "a@b.com" }
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("id", "1");
    data.put("name", "Alice");
    data.put("email", "a@b.com");

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    assertEquals(shape.getFieldCount(), 3);
  }

  @Test
  public void testFlatResponse_normalizedShapeFormat() {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("id", "1");
    data.put("name", "Alice");

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // Scalars should appear as key_ entries inside braces
    String s = shape.getNormalizedShape();
    assertTrue(s.startsWith("{"), "Shape should start with '{'");
    assertTrue(s.contains("id_"), "Shape should contain 'id_'");
    assertTrue(s.contains("name_"), "Shape should contain 'name_'");
  }

  // -------------------------------------------------------------------------
  // Nested response
  // -------------------------------------------------------------------------

  @Test
  public void testNestedResponse_normalizedShapeContainsNesting() {
    // { "user": { "id": "1", "name": "Alice" } }
    Map<String, Object> user = new LinkedHashMap<>();
    user.put("id", "1");
    user.put("name", "Alice");

    Map<String, Object> data = new LinkedHashMap<>();
    data.put("user", user);

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // field count = 2 (id, name)
    assertEquals(shape.getFieldCount(), 2);
    String s = shape.getNormalizedShape();
    // nested object should be reflected with dot notation: user.{...}
    assertTrue(s.contains("user.{"), "Shape should contain nested 'user.{', got: " + s);
  }

  // -------------------------------------------------------------------------
  // Array response
  // -------------------------------------------------------------------------

  @Test
  public void testArrayResponse_sizeInShape() {
    // { "items": ["a", "b", "c"] }
    List<Object> items = Arrays.asList("a", "b", "c");
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("items", items);

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    assertTrue(
        shape.getNormalizedShape().contains("[3]"),
        "Shape should contain '[3]', got: " + shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("_"),
        "Shape should show scalar shapes (underscore), got: " + shape.getNormalizedShape());
  }

  @Test
  public void testArrayResponse_maxArraySizeCaptured() {
    // { "small": [1,2], "big": [10, 20, 30, 40, 50] }
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("small", Arrays.asList("a", "b"));
    data.put("big", Arrays.asList(1, 2, 3, 4, 5));

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // Verify both array sizes are captured in the normalized shape
    assertTrue(
        shape.getNormalizedShape().contains("[2]"),
        "Shape should contain small array size [2], got: " + shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("[5]"),
        "Shape should contain big array size [5], got: " + shape.getNormalizedShape());

    // Verify heavy fields (arrays > 1 element) are tracked
    assertTrue(shape.getHeavyFields().size() > 0, "Heavy fields should contain large arrays");
    // At least one field should be "big" with size 5
    boolean hasBigField =
        shape.getHeavyFields().stream().anyMatch(f -> "big".equals(f.path()) && 5 == f.size());
    assertTrue(hasBigField, "Heavy fields should include 'big' array with size 5");
  }

  @Test
  public void testEmptyArray_countedButSizeZero() {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("items", Collections.emptyList());

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    assertTrue(shape.getNormalizedShape().contains("[0]"));
  }

  // -------------------------------------------------------------------------
  // Mixed types (List of Maps)
  // -------------------------------------------------------------------------

  @Test
  public void testListOfMaps_handledCorrectly() {
    // { "users": [ { "id": "1", "name": "Alice" }, { "id": "2", "name": "Bob" } ] }
    Map<String, Object> user1 = new LinkedHashMap<>();
    user1.put("id", "1");
    user1.put("name", "Alice");

    Map<String, Object> user2 = new LinkedHashMap<>();
    user2.put("id", "2");
    user2.put("name", "Bob");

    Map<String, Object> data = new LinkedHashMap<>();
    data.put("users", Arrays.asList(user1, user2));

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // Averages field count across sampled elements (both have 2 fields) = 2 * array size (2) = 4
    // total fields
    assertEquals(shape.getFieldCount(), 4);
    // Shape shows array size followed by dot notation and object shapes: [2].{id name}
    assertTrue(
        shape.getNormalizedShape().contains("[2]"),
        "Shape should contain array size [2], got: " + shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("id"),
        "Shape should contain field name 'id', got: " + shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("name"),
        "Shape should contain field name 'name', got: " + shape.getNormalizedShape());
  }

  @Test
  public void testLargeArrayOfObjects_fieldCountEstimated() {
    // { "items": [ { "id": "1", "name": "Alice", "email": "a@b.com" }, ... ] } with 100 items
    // Each item has 3 fields, average across samples * array size = estimated total
    Map<String, Object> item = new LinkedHashMap<>();
    item.put("id", "1");
    item.put("name", "Alice");
    item.put("email", "a@b.com");

    List<Object> items = new java.util.ArrayList<>();
    for (int i = 0; i < 100; i++) {
      items.add(item);
    }

    Map<String, Object> data = new LinkedHashMap<>();
    data.put("items", items);

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // Field count: average (3 fields per item) * 100 (array size) = 300
    assertEquals(shape.getFieldCount(), 300);
    assertTrue(
        shape.getNormalizedShape().contains("[100]"),
        "Shape should contain array size [100], got: " + shape.getNormalizedShape());
  }

  @Test
  public void testNestedArrays_fieldCountCompounds() {
    // { "rows": [ {"cells": ["a","b","c"]}, {"cells": ["d","e","f"]} ] }
    // Inner arrays have 3 elements each, outer array has 2 elements
    // Averages field count across sampled outer elements * outer array size
    // Then inner array (3 elements) is multiplied as well = 6+ total fields
    Map<String, Object> row1 = new LinkedHashMap<>();
    row1.put("cells", Arrays.asList("a", "b", "c"));

    Map<String, Object> row2 = new LinkedHashMap<>();
    row2.put("cells", Arrays.asList("d", "e", "f"));

    Map<String, Object> data = new LinkedHashMap<>();
    data.put("rows", Arrays.asList(row1, row2));

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    // Inner array (3 elements) multiplied by outer array (2 elements) = 6+ fields
    assertEquals(shape.getFieldCount(), 6);
    assertTrue(
        shape.getNormalizedShape().contains("[2]"),
        "Shape should contain outer array size [2], got: " + shape.getNormalizedShape());
    assertTrue(
        shape.getNormalizedShape().contains("[3]"),
        "Shape should contain inner array size [3], got: " + shape.getNormalizedShape());
  }

  // -------------------------------------------------------------------------
  // Depth cap (MAX_DEPTH = 10)
  // -------------------------------------------------------------------------

  @Test
  public void testDepthCap_shapeTruncatedAtMaxDepth() {
    // Build a deeply nested map exceeding MAX_DEPTH=10
    Map<String, Object> current = new LinkedHashMap<>();
    current.put("leaf", "value");

    // Wrap 12 times to exceed the cap
    for (int i = 11; i >= 0; i--) {
      Map<String, Object> wrapper = new LinkedHashMap<>();
      wrapper.put("level" + i, current);
      current = wrapper;
    }

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(current);

    // Should not throw StackOverflowError
    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);
    assertNotNull(shape);
    // The truncation marker should appear somewhere in the shape
    assertTrue(
        shape.getNormalizedShape().contains("..."),
        "Deep shape should contain '...' truncation marker");
  }

  // -------------------------------------------------------------------------
  // Shape truncation at MAX_SHAPE_LENGTH (2048)
  // -------------------------------------------------------------------------

  @Test
  public void testShapeTruncation_cappedAt2048Chars() {
    // Build a wide flat map with enough keys to exceed 2048 chars
    Map<String, Object> data = new LinkedHashMap<>();
    for (int i = 0; i < 300; i++) {
      data.put("someLongFieldName" + i, "value");
    }

    ExecutionResult result = mock(ExecutionResult.class);
    when(result.getData()).thenReturn(data);

    ResponseShapeAnalyzer.ResponseShape shape = ResponseShapeAnalyzer.analyze(result);

    assertTrue(
        shape.getNormalizedShape().length() <= 2048 + 3,
        "Shape should be truncated, length=" + shape.getNormalizedShape().length());
    assertTrue(shape.getNormalizedShape().endsWith("..."), "Truncated shape should end with '...'");
  }
}
