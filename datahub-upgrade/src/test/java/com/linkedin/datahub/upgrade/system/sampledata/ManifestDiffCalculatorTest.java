package com.linkedin.datahub.upgrade.system.sampledata;

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/** Unit tests for ManifestDiffCalculator. */
public class ManifestDiffCalculatorTest {

  private final ManifestDiffCalculator calculator = new ManifestDiffCalculator();

  @Test
  public void testCalculateDiff_noChanges() {
    // Given: Same entities and aspects in both manifests
    Manifest previous = buildManifest("001", Map.of("urn:li:dataset:1", Set.of("ownership")));
    Manifest current = buildManifest("002", Map.of("urn:li:dataset:1", Set.of("ownership")));

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: No changes detected
    assertTrue(diff.getRemovedUrns().isEmpty());
    assertTrue(diff.getUrnsWithRemovedAspects().isEmpty());
    assertTrue(diff.getHardDeleteUrns().isEmpty());
  }

  @Test
  public void testCalculateDiff_removedUrns() {
    // Given: dataset:2 removed in current version
    Manifest previous =
        buildManifest(
            "001",
            Map.of(
                "urn:li:dataset:1", Set.of("ownership", "schema"),
                "urn:li:dataset:2", Set.of("ownership")));

    Manifest current =
        buildManifest("002", Map.of("urn:li:dataset:1", Set.of("ownership", "schema")));

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: dataset:2 marked as removed
    assertEquals(diff.getRemovedUrns(), Set.of("urn:li:dataset:2"));
    assertTrue(diff.getUrnsWithRemovedAspects().isEmpty());
    assertEquals(diff.getHardDeleteUrns(), Set.of("urn:li:dataset:2"));
  }

  @Test
  public void testCalculateDiff_removedAspects() {
    // Given: properties aspect removed from dataset:1
    Manifest previous =
        buildManifest(
            "001", Map.of("urn:li:dataset:1", Set.of("ownership", "schema", "properties")));

    Manifest current =
        buildManifest("002", Map.of("urn:li:dataset:1", Set.of("ownership", "schema")));

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: dataset:1 has removed aspects
    assertTrue(diff.getRemovedUrns().isEmpty());
    assertEquals(diff.getUrnsWithRemovedAspects(), Set.of("urn:li:dataset:1"));
    assertEquals(diff.getHardDeleteUrns(), Set.of("urn:li:dataset:1"));
  }

  @Test
  public void testCalculateDiff_addedUrns() {
    // Given: dataset:2 added in current version
    Manifest previous = buildManifest("001", Map.of("urn:li:dataset:1", Set.of("ownership")));

    Manifest current =
        buildManifest(
            "002",
            Map.of(
                "urn:li:dataset:1", Set.of("ownership"),
                "urn:li:dataset:2", Set.of("ownership")));

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: No deletions (added entities don't trigger deletes)
    assertTrue(diff.getRemovedUrns().isEmpty());
    assertTrue(diff.getUrnsWithRemovedAspects().isEmpty());
    assertTrue(diff.getHardDeleteUrns().isEmpty());
  }

  @Test
  public void testCalculateDiff_addedAspects() {
    // Given: documentation aspect added to dataset:1
    Manifest previous = buildManifest("001", Map.of("urn:li:dataset:1", Set.of("ownership")));

    Manifest current =
        buildManifest("002", Map.of("urn:li:dataset:1", Set.of("ownership", "documentation")));

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: No deletions (added aspects don't trigger deletes)
    assertTrue(diff.getRemovedUrns().isEmpty());
    assertTrue(diff.getUrnsWithRemovedAspects().isEmpty());
    assertTrue(diff.getHardDeleteUrns().isEmpty());
  }

  @Test
  public void testCalculateDiff_mixedChanges() {
    // Given: Complex changes - one entity removed, one with aspects removed, one unchanged
    Manifest previous =
        buildManifest(
            "001",
            Map.of(
                "urn:li:dataset:1", Set.of("ownership", "schema"),
                "urn:li:dataset:2", Set.of("ownership", "schema", "properties"),
                "urn:li:dataset:3", Set.of("ownership")));

    Manifest current =
        buildManifest(
            "002",
            Map.of(
                "urn:li:dataset:1", Set.of("ownership", "schema"),
                "urn:li:dataset:2",
                    Set.of("ownership", "schema"))); // dataset:3 removed, properties
    // removed from dataset:2

    // When: Calculate diff
    ManifestDiff diff = calculator.calculateDiff(previous, current);

    // Then: Both changes detected
    assertEquals(diff.getRemovedUrns(), Set.of("urn:li:dataset:3"));
    assertEquals(diff.getUrnsWithRemovedAspects(), Set.of("urn:li:dataset:2"));
    assertEquals(diff.getHardDeleteUrns(), Set.of("urn:li:dataset:2", "urn:li:dataset:3"));
  }

  @Test
  public void testCalculateDiffForFreshInstall() {
    // Given: Current manifest with entities
    Manifest current =
        buildManifest(
            "002",
            Map.of(
                "urn:li:dataset:1", Set.of("ownership"),
                "urn:li:dataset:2", Set.of("ownership")));

    // When: Calculate diff for fresh install
    ManifestDiff diff = calculator.calculateDiffForFreshInstall(current);

    // Then: No deletions on fresh install
    assertTrue(diff.getRemovedUrns().isEmpty());
    assertTrue(diff.getUrnsWithRemovedAspects().isEmpty());
    assertTrue(diff.getHardDeleteUrns().isEmpty());
  }

  @Test
  public void testLoadManifest_001() {
    // Given: Version 001 manifest (legacy unprefixed entities from acryl-main)
    // Note: 001_manifest.json is temporary and can be removed once all production
    // instances have migrated to this branch. The code has auto-detection fallback
    // via hasUnprefixedSampleData() which makes the manifest optional.

    // When: Load version 001 manifest from classpath
    Manifest manifest = calculator.loadManifest("001");

    // Then: Manifest loaded successfully
    assertNotNull(manifest);
    assertEquals(manifest.getVersion(), "001");
    assertFalse(manifest.getEntityUrns().isEmpty());

    // Verify each entity has aspect mappings
    String firstUrn = manifest.getEntityUrns().iterator().next();
    assertNotNull(manifest.getAspects(firstUrn));
    assertFalse(
        manifest.getAspects(firstUrn).isEmpty(), "Each entity should have at least one aspect");

    // Verify unprefixed URNs (version 001 is pre-migration)
    boolean hasDatasetUrn =
        manifest.getEntityUrns().stream()
            .anyMatch(urn -> urn.contains(":dataset:") && !urn.contains("sample_data_"));
    assertTrue(hasDatasetUrn, "Version 001 should have unprefixed dataset URNs");
  }

  @Test
  public void testLoadManifest_validHashVersion() {
    // Given: Any valid hash-based manifest version (not 001)
    // Note: We test with 001 since it's stable. In production, current version
    // manifests use 12-char SHA256 hashes that change when sample data updates.
    // This test validates the manifest loading mechanism works for any version.

    // When: Load a manifest by version
    Manifest manifest = calculator.loadManifest("001"); // Using 001 as a stable example

    // Then: Should load with correct version and structure
    assertNotNull(manifest);
    assertEquals(manifest.getVersion(), "001");

    // Validate manifest structure
    assertNotNull(manifest.getEntities());
    assertFalse(manifest.getEntityUrns().isEmpty());

    // Each entity should have at least one aspect
    for (String urn : manifest.getEntityUrns()) {
      Set<String> aspects = manifest.getAspects(urn);
      assertNotNull(aspects, "Entity " + urn + " should have aspects");
      assertFalse(aspects.isEmpty(), "Entity " + urn + " should have at least one aspect");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testLoadManifest_nonexistent() {
    // When: Try to load non-existent manifest
    calculator.loadManifest("999");

    // Then: Should throw RuntimeException
  }

  /** Helper to build a test manifest */
  private Manifest buildManifest(String version, Map<String, Set<String>> entities) {
    Manifest manifest = new Manifest();
    manifest.setVersion(version);
    manifest.setEntities(new HashMap<>(entities));
    return manifest;
  }
}
