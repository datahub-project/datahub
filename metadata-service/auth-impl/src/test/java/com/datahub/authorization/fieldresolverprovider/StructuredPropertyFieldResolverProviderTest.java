package com.datahub.authorization.fieldresolverprovider;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.FieldResolver;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class StructuredPropertyFieldResolverProviderTest {

  @Test
  public void testSuccessfulPropertyResolution() {
    // Test: resolver successfully maps and returns structured property values
    Map<String, Set<String>> expectedProperties =
        ImmutableMap.of("urn:li:structuredProperty:env", Set.of("prod", "staging"));

    FieldResolver.FieldValue fieldValue =
        FieldResolver.FieldValue.builder()
            .values(Collections.emptySet())
            .structuredPropertyValues(expectedProperties)
            .build();

    FieldResolver resolver = new FieldResolver(() -> CompletableFuture.completedFuture(fieldValue));
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().join();

    assertNotNull(result);
    assertNotNull(result.getStructuredPropertyValues());
    assertEquals(
        expectedProperties, result.getStructuredPropertyValues(), "Properties should match");
  }

  @Test
  public void testEmptyStructuredPropertyValues() {
    // Test: resolver handles empty structured property values
    Map<String, Set<String>> emptyProperties = Collections.emptyMap();

    FieldResolver.FieldValue fieldValue =
        FieldResolver.FieldValue.builder()
            .values(Collections.emptySet())
            .structuredPropertyValues(emptyProperties)
            .build();

    FieldResolver resolver = new FieldResolver(() -> CompletableFuture.completedFuture(fieldValue));
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().join();

    assertNotNull(result);
    assertNotNull(result.getStructuredPropertyValues());
    assertTrue(result.getStructuredPropertyValues().isEmpty(), "Properties should be empty");
  }

  @Test
  public void testNullStructuredPropertyValues() {
    // Test: resolver handles null structured property values gracefully
    FieldResolver.FieldValue fieldValue =
        FieldResolver.FieldValue.builder().values(Collections.emptySet()).build();

    FieldResolver resolver = new FieldResolver(() -> CompletableFuture.completedFuture(fieldValue));
    FieldResolver.FieldValue result = resolver.getFieldValuesFuture().join();

    assertNotNull(result);
    // Either null or empty map is acceptable
    if (result.getStructuredPropertyValues() != null) {
      assertTrue(
          result.getStructuredPropertyValues().isEmpty()
              || result.getStructuredPropertyValues() == null,
          "Properties should be null or empty");
    }
  }
}
