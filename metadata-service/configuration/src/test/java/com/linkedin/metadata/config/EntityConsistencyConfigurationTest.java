package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class EntityConsistencyConfigurationTest {

  @Test
  public void testModeFromString() {
    assertEquals(ConsistencyCheckMode.fromString("active"), ConsistencyCheckMode.ACTIVE);
    assertEquals(ConsistencyCheckMode.fromString("dry-run"), ConsistencyCheckMode.DRY_RUN);
    assertEquals(ConsistencyCheckMode.fromString("DRY_RUN"), ConsistencyCheckMode.DRY_RUN);
    assertEquals(ConsistencyCheckMode.fromString("disabled"), ConsistencyCheckMode.DISABLED);
    assertEquals(ConsistencyCheckMode.fromString(null), ConsistencyCheckMode.DRY_RUN);
    expectThrows(IllegalArgumentException.class, () -> ConsistencyCheckMode.fromString("bogus"));
  }

  @Test
  public void testScheduleFromString() {
    assertEquals(ConsistencyCheckSchedule.fromString("monthly"), ConsistencyCheckSchedule.MONTHLY);
    assertEquals(
        ConsistencyCheckSchedule.fromString("every-run"), ConsistencyCheckSchedule.EVERY_RUN);
    assertEquals(ConsistencyCheckSchedule.fromString(null), ConsistencyCheckSchedule.EVERY_RUN);
    expectThrows(
        IllegalArgumentException.class, () -> ConsistencyCheckSchedule.fromString("yearly"));
  }

  @Test
  public void testShouldApplyFixesRespectsJobCeiling() {
    EntityConsistencyConfiguration config = new EntityConsistencyConfiguration();
    config.setDryRun(true);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    config.setChecks(Map.of("orphan-index-document", orphan));

    assertFalse(config.shouldApplyFixes("orphan-index-document"));
  }

  @Test
  public void testShouldApplyFixesWhenActiveAndCeilingOff() {
    EntityConsistencyConfiguration config = new EntityConsistencyConfiguration();
    config.setDryRun(false);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    EntityConsistencyConfiguration.CheckRunConfig assertion =
        new EntityConsistencyConfiguration.CheckRunConfig();
    assertion.setMode("dry-run");
    config.setChecks(Map.of("orphan-index-document", orphan, "assertion-invalid-type", assertion));

    assertTrue(config.shouldApplyFixes("orphan-index-document"));
    assertFalse(config.shouldApplyFixes("assertion-invalid-type"));
    // Unconfigured check defaults to dry-run
    assertFalse(config.shouldApplyFixes("unknown-check"));
  }

  @Test
  public void testResolvedOrphanDefaultsFromYamlShape() {
    EntityConsistencyConfiguration config = new EntityConsistencyConfiguration();
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    orphan.setSchedule("monthly");
    config.setChecks(Map.of("orphan-index-document", orphan));

    assertEquals(config.getCheckMode("orphan-index-document"), ConsistencyCheckMode.ACTIVE);
    assertEquals(
        config.getCheckSchedule("orphan-index-document"), ConsistencyCheckSchedule.MONTHLY);
  }

  @Test
  public void testKeyAspectOnlyFilterConfig() {
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filter =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    assertFalse(filter.isKeyAspectOnly());
    assertFalse(filter.hasAnyConfig());

    filter.setKeyAspectOnly(true);
    assertTrue(filter.isKeyAspectOnly());
    assertTrue(filter.hasAnyConfig());
  }

  @Test
  public void testSystemMetadataFilterEntityTypes() {
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filter =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    assertFalse(filter.hasEntityTypes());

    filter.setEntityTypes(List.of("dataset", "query"));
    assertTrue(filter.hasEntityTypes());
    assertTrue(filter.hasAnyConfig());
    assertEquals(filter.getEntityTypes().size(), 2);
  }
}
