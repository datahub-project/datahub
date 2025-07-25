package com.datahub.gms.servlet;

import static org.testng.Assert.*;

import java.util.Set;
import org.testng.annotations.Test;

/**
 * Unit tests for ConfigSectionRule class.
 *
 * <p>These tests verify: - Rule creation methods - Field path parsing and categorization - Nested
 * path detection and retrieval - Edge cases and validation
 */
public class ConfigSectionRuleTest {

  @Test
  public void testIncludeAllFields() {
    ConfigSectionRule rule = ConfigSectionRule.include("authentication");

    assertEquals(rule.getSectionPath(), "authentication");
    assertEquals(rule.getOutputPath(), "authentication");
    assertTrue(rule.isIncludeSection());
    assertTrue(rule.isAllFieldsAllowed());
    assertNull(rule.getAllowedFields());

    // When all fields are allowed, any field should be allowed
    assertTrue(rule.isFieldAllowed("anyField"));
    assertTrue(rule.isFieldAllowed("sensitive.field"));
  }

  @Test
  public void testIncludeWithSpecificFields() {
    Set<String> allowedFields = Set.of("enabled", "defaultProvider");
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);

    assertEquals(rule.getSectionPath(), "authentication");
    assertEquals(rule.getOutputPath(), "authentication");
    assertTrue(rule.isIncludeSection());
    assertFalse(rule.isAllFieldsAllowed());
    assertEquals(rule.getAllowedFields(), allowedFields);

    // Only specific fields should be allowed
    assertTrue(rule.isFieldAllowed("enabled"));
    assertTrue(rule.isFieldAllowed("defaultProvider"));
    assertFalse(rule.isFieldAllowed("secretField"));
  }

  @Test
  public void testIncludeWithRenaming() {
    Set<String> allowedFields = Set.of("enabled");
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", "auth", allowedFields);

    assertEquals(rule.getSectionPath(), "authentication");
    assertEquals(rule.getOutputPath(), "auth"); // Should use renamed output path
    assertTrue(rule.isIncludeSection());
    assertEquals(rule.getAllowedFields(), allowedFields);
  }

  @Test
  public void testExcludeSection() {
    ConfigSectionRule rule = ConfigSectionRule.exclude("authentication");

    assertEquals(rule.getSectionPath(), "authentication");
    assertEquals(rule.getOutputPath(), "authentication");
    assertFalse(rule.isIncludeSection());

    // Excluded sections should not allow any fields
    assertFalse(rule.isFieldAllowed("anyField"));
    assertFalse(rule.isFieldAllowed("enabled"));
  }

  @Test
  public void testTopLevelFieldDetection() {
    Set<String> allowedFields =
        Set.of(
            "enabled", // Top-level
            "defaultProvider", // Top-level
            "tokenService.signingAlgorithm", // Nested
            "tokenService.issuer", // Nested
            "deep.nested.field" // Deep nested
            );

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);
    Set<String> topLevelFields = rule.getTopLevelFields();

    assertEquals(topLevelFields.size(), 2);
    assertTrue(topLevelFields.contains("enabled"));
    assertTrue(topLevelFields.contains("defaultProvider"));
    assertFalse(topLevelFields.contains("tokenService.signingAlgorithm"));
  }

  @Test
  public void testNestedPathDetection() {
    Set<String> allowedFields =
        Set.of(
            "enabled", // Top-level
            "tokenService.signingAlgorithm", // Nested under tokenService
            "tokenService.issuer", // Nested under tokenService
            "cache.settings.ttl", // Nested under cache
            "notNested" // Top-level
            );

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);

    // Should detect nested paths for tokenService
    assertTrue(rule.hasNestedPathsForField("tokenService"));

    // Should detect nested paths for cache
    assertTrue(rule.hasNestedPathsForField("cache"));

    // Should not detect nested paths for top-level fields
    assertFalse(rule.hasNestedPathsForField("enabled"));
    assertFalse(rule.hasNestedPathsForField("notNested"));

    // Should not detect nested paths for non-existent fields
    assertFalse(rule.hasNestedPathsForField("nonExistent"));
  }

  @Test
  public void testGetAllowedPathsWithPrefix() {
    Set<String> allowedFields =
        Set.of(
            "enabled", // Top-level - should not match
            "tokenService.signingAlgorithm", // Should match tokenService prefix
            "tokenService.issuer", // Should match tokenService prefix
            "tokenService.audience", // Should match tokenService prefix
            "cache.settings.ttl", // Should match cache prefix but not tokenService
            "otherService.field" // Should not match tokenService prefix
            );

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);

    // Get paths with tokenService prefix
    Set<String> tokenServicePaths = rule.getAllowedPathsWithPrefix("tokenService");
    assertEquals(tokenServicePaths.size(), 3);
    assertTrue(tokenServicePaths.contains("tokenService.signingAlgorithm"));
    assertTrue(tokenServicePaths.contains("tokenService.issuer"));
    assertTrue(tokenServicePaths.contains("tokenService.audience"));
    assertFalse(tokenServicePaths.contains("enabled"));
    assertFalse(tokenServicePaths.contains("cache.settings.ttl"));

    // Get paths with cache prefix
    Set<String> cachePaths = rule.getAllowedPathsWithPrefix("cache");
    assertEquals(cachePaths.size(), 1);
    assertTrue(cachePaths.contains("cache.settings.ttl"));

    // Get paths with non-existent prefix
    Set<String> nonExistentPaths = rule.getAllowedPathsWithPrefix("nonExistent");
    assertTrue(nonExistentPaths.isEmpty());
  }

  @Test
  public void testEmptyAllowedFields() {
    Set<String> emptyFields = Set.of();
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", emptyFields);

    assertFalse(rule.isAllFieldsAllowed());
    assertEquals(rule.getAllowedFields().size(), 0);

    // No fields should be allowed
    assertFalse(rule.isFieldAllowed("anyField"));

    // No top-level fields
    assertTrue(rule.getTopLevelFields().isEmpty());

    // No nested paths
    assertFalse(rule.hasNestedPathsForField("anyField"));
    assertTrue(rule.getAllowedPathsWithPrefix("anyPrefix").isEmpty());
  }

  @Test
  public void testSingleNestedPath() {
    Set<String> allowedFields = Set.of("tokenService.signingAlgorithm");
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);

    // Should have no top-level fields
    assertTrue(rule.getTopLevelFields().isEmpty());

    // Should detect nested path for tokenService
    assertTrue(rule.hasNestedPathsForField("tokenService"));

    // Should return the single nested path
    Set<String> paths = rule.getAllowedPathsWithPrefix("tokenService");
    assertEquals(paths.size(), 1);
    assertTrue(paths.contains("tokenService.signingAlgorithm"));
  }

  @Test
  public void testDeepNestedPaths() {
    Set<String> allowedFields =
        Set.of(
            "level1.level2.level3.field1",
            "level1.level2.level3.field2",
            "level1.level2.otherField",
            "level1.directField");

    ConfigSectionRule rule = ConfigSectionRule.include("test", allowedFields);

    // Should detect nested paths for level1
    assertTrue(rule.hasNestedPathsForField("level1"));

    // Should return all paths starting with level1
    Set<String> level1Paths = rule.getAllowedPathsWithPrefix("level1");
    assertEquals(level1Paths.size(), 4);
    assertTrue(level1Paths.contains("level1.level2.level3.field1"));
    assertTrue(level1Paths.contains("level1.level2.level3.field2"));
    assertTrue(level1Paths.contains("level1.level2.otherField"));
    assertTrue(level1Paths.contains("level1.directField"));

    // Should return subset for level1.level2 prefix
    Set<String> level2Paths = rule.getAllowedPathsWithPrefix("level1.level2");
    assertEquals(level2Paths.size(), 3);
    assertTrue(level2Paths.contains("level1.level2.level3.field1"));
    assertTrue(level2Paths.contains("level1.level2.level3.field2"));
    assertTrue(level2Paths.contains("level1.level2.otherField"));
    assertFalse(level2Paths.contains("level1.directField"));
  }

  @Test
  public void testAllFieldsAllowedOverridesSpecificRules() {
    // When allowedFields is null (all fields allowed), specific field queries should return true
    ConfigSectionRule rule = ConfigSectionRule.include("authentication");

    assertTrue(rule.isAllFieldsAllowed());

    // These methods should return empty for "allow all" rules since they're not needed
    assertTrue(rule.getTopLevelFields().isEmpty());
    assertFalse(rule.hasNestedPathsForField("anyField"));
    assertTrue(rule.getAllowedPathsWithPrefix("anyPrefix").isEmpty());

    // But isFieldAllowed should still return true for any field
    assertTrue(rule.isFieldAllowed("anyField"));
  }

  @Test
  public void testToStringMethod() {
    Set<String> allowedFields = Set.of("enabled", "tokenService.issuer");
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", "auth", allowedFields);

    String toString = rule.toString();

    // Should contain key information
    assertTrue(toString.contains("authentication"));
    assertTrue(toString.contains("auth"));
    assertTrue(toString.contains("true")); // includeSection
    assertTrue(toString.contains("enabled"));
    assertTrue(toString.contains("tokenService.issuer"));
  }

  @Test
  public void testExcludedSectionBehavior() {
    ConfigSectionRule rule = ConfigSectionRule.exclude("authentication");

    assertFalse(rule.isIncludeSection());

    // All helper methods should indicate no allowed fields/paths
    assertFalse(rule.isFieldAllowed("anyField"));
    assertTrue(rule.getTopLevelFields().isEmpty());
    assertFalse(rule.hasNestedPathsForField("anyField"));
    assertTrue(rule.getAllowedPathsWithPrefix("anyPrefix").isEmpty());
  }
}
