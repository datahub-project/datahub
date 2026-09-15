package com.datahub.authorization;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

class ResolvedEntitySpecTest {

  @Test
  void testGetStructuredPropertyValues_Success() {
    // Test: successful resolution returns the map
    Map<String, Set<String>> expectedProperties =
        Map.of("urn:li:structuredPropertyDefinition:env", Set.of("prod", "staging"));

    FieldResolver.FieldValue fieldValue =
        FieldResolver.FieldValue.builder()
            .values(Collections.emptySet())
            .structuredPropertyValues(expectedProperties)
            .build();

    FieldResolver fieldResolver =
        new FieldResolver(() -> CompletableFuture.completedFuture(fieldValue));

    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");
    final ResolvedEntitySpec resolvedSpec =
        new ResolvedEntitySpec(
            spec, ImmutableMap.of(EntityFieldType.STRUCTURED_PROPERTY, fieldResolver));

    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertEquals(expectedProperties, result);
  }

  @Test
  void testGetStructuredPropertyValues_TimeoutReturnsEmpty() {
    // Test: timeout should return empty map, not throw
    CompletableFuture<FieldResolver.FieldValue> future = new CompletableFuture<>();
    future.completeExceptionally(new TimeoutException("Timeout resolving properties"));

    FieldResolver fieldResolver = new FieldResolver(() -> future);

    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");
    final ResolvedEntitySpec resolvedSpec =
        new ResolvedEntitySpec(
            spec, ImmutableMap.of(EntityFieldType.STRUCTURED_PROPERTY, fieldResolver));

    // Should not throw, should return empty map
    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map on timeout, not throw exception");
  }

  @Test
  void testGetStructuredPropertyValues_FailedFutureReturnsEmpty() {
    // Test: failed future should return empty map, not throw
    CompletableFuture<FieldResolver.FieldValue> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Failed to resolve structured properties"));

    FieldResolver fieldResolver = new FieldResolver(() -> future);

    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");
    final ResolvedEntitySpec resolvedSpec =
        new ResolvedEntitySpec(
            spec, ImmutableMap.of(EntityFieldType.STRUCTURED_PROPERTY, fieldResolver));

    // Should not throw, should return empty map
    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map on error, not throw exception");
  }

  @Test
  void testGetStructuredPropertyValues_InterruptedReturnsEmpty() {
    // Test: interrupted exception should return empty map without propagating (exception handling)
    CompletableFuture<FieldResolver.FieldValue> future = new CompletableFuture<>();
    future.completeExceptionally(new InterruptedException("Thread interrupted"));

    FieldResolver fieldResolver = new FieldResolver(() -> future);

    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");
    final ResolvedEntitySpec resolvedSpec =
        new ResolvedEntitySpec(
            spec, ImmutableMap.of(EntityFieldType.STRUCTURED_PROPERTY, fieldResolver));

    // Should not throw, should return empty map gracefully
    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map on InterruptedException, not propagate");
  }

  @Test
  void testGetStructuredPropertyValues_NoResolverReturnsEmpty() {
    // Test: no STRUCTURED_PROPERTY resolver should return empty map
    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");

    final ResolvedEntitySpec resolvedSpec = new ResolvedEntitySpec(spec, Collections.emptyMap());

    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map when no STRUCTURED_PROPERTY resolver");
  }

  @Test
  void testGetStructuredPropertyValues_NullResultReturnsEmpty() {
    // Test: null structured property result should return empty map
    FieldResolver.FieldValue fieldValue =
        FieldResolver.FieldValue.builder()
            .values(Collections.emptySet())
            .structuredPropertyValues(null)
            .build();

    FieldResolver fieldResolver =
        new FieldResolver(() -> CompletableFuture.completedFuture(fieldValue));

    final EntitySpec spec = new EntitySpec("dataset", "urn:li:dataset:test");
    final ResolvedEntitySpec resolvedSpec =
        new ResolvedEntitySpec(
            spec, ImmutableMap.of(EntityFieldType.STRUCTURED_PROPERTY, fieldResolver));

    Map<String, Set<String>> result = resolvedSpec.getStructuredPropertyValues();

    assertNotNull(result);
    assertTrue(result.isEmpty(), "Should return empty map when structured property result is null");
  }
}
