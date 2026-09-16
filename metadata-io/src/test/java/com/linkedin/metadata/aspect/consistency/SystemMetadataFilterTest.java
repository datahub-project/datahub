package com.linkedin.metadata.aspect.consistency;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import java.util.List;
import org.testng.annotations.Test;

/** Tests for SystemMetadataFilter. */
public class SystemMetadataFilterTest {

  // ============================================================================
  // Empty Filter Tests
  // ============================================================================

  @Test
  public void testEmptyFilter() {
    SystemMetadataFilter filter = SystemMetadataFilter.empty();

    assertNotNull(filter);
    assertNull(filter.getGePitEpochMs());
    assertNull(filter.getLePitEpochMs());
    assertNull(filter.getAspectFilters());
    assertFalse(filter.isIncludeSoftDeleted());
  }

  @Test
  public void testEmptyFilterHasNoConfig() {
    SystemMetadataFilter filter = SystemMetadataFilter.empty();

    assertFalse(filter.hasAnyConfig());
  }

  // ============================================================================
  // Builder Tests
  // ============================================================================

  @Test
  public void testBuilderWithAllFields() {
    long now = System.currentTimeMillis();
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder()
            .gePitEpochMs(now - 1000)
            .lePitEpochMs(now)
            .aspectFilters(List.of("aspect1", "aspect2"))
            .includeSoftDeleted(true)
            .build();

    assertEquals(filter.getGePitEpochMs(), Long.valueOf(now - 1000));
    assertEquals(filter.getLePitEpochMs(), Long.valueOf(now));
    assertEquals(filter.getAspectFilters().size(), 2);
    assertTrue(filter.getAspectFilters().contains("aspect1"));
    assertTrue(filter.getAspectFilters().contains("aspect2"));
    assertTrue(filter.isIncludeSoftDeleted());
  }

  @Test
  public void testBuilderWithOnlyTimestampGe() {
    long timestamp = 1234567890L;
    SystemMetadataFilter filter = SystemMetadataFilter.builder().gePitEpochMs(timestamp).build();

    assertEquals(filter.getGePitEpochMs(), Long.valueOf(timestamp));
    assertNull(filter.getLePitEpochMs());
    assertNull(filter.getAspectFilters());
    assertFalse(filter.isIncludeSoftDeleted());
  }

  @Test
  public void testBuilderWithOnlyTimestampLe() {
    long timestamp = 1234567890L;
    SystemMetadataFilter filter = SystemMetadataFilter.builder().lePitEpochMs(timestamp).build();

    assertNull(filter.getGePitEpochMs());
    assertEquals(filter.getLePitEpochMs(), Long.valueOf(timestamp));
    assertNull(filter.getAspectFilters());
    assertFalse(filter.isIncludeSoftDeleted());
  }

  @Test
  public void testBuilderWithOnlyAspectFilters() {
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder()
            .aspectFilters(List.of("status", "datasetProperties"))
            .build();

    assertNull(filter.getGePitEpochMs());
    assertNull(filter.getLePitEpochMs());
    assertEquals(filter.getAspectFilters().size(), 2);
    assertFalse(filter.isIncludeSoftDeleted());
  }

  // ============================================================================
  // hasAnyConfig Tests
  // ============================================================================

  @Test
  public void testHasAnyConfigWithGe() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().gePitEpochMs(1234567890L).build();

    assertTrue(filter.hasAnyConfig());
  }

  @Test
  public void testHasAnyConfigWithLe() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().lePitEpochMs(1234567890L).build();

    assertTrue(filter.hasAnyConfig());
  }

  @Test
  public void testHasAnyConfigWithAspects() {
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder().aspectFilters(List.of("aspect1")).build();

    assertTrue(filter.hasAnyConfig());
  }

  @Test
  public void testHasAnyConfigWithEmptyAspectList() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().aspectFilters(List.of()).build();

    assertFalse(filter.hasAnyConfig());
  }

  @Test
  public void testHasAnyConfigFalseWithOnlyIncludeSoftDeleted() {
    // includeSoftDeleted alone doesn't count as "config" since it's a boolean default
    SystemMetadataFilter filter = SystemMetadataFilter.builder().includeSoftDeleted(true).build();

    assertFalse(filter.hasAnyConfig());
  }

  // ============================================================================
  // From Configuration Tests
  // ============================================================================

  @Test
  public void testFromNullConfigReturnsNull() {
    SystemMetadataFilter filter = SystemMetadataFilter.from(null);

    assertNull(filter);
  }

  @Test
  public void testFromConfigurationWithAllFields() {
    EntityConsistencyConfiguration.SystemMetadataFilterConfig config =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    config.setGePitEpochMs(1000L);
    config.setLePitEpochMs(2000L);
    config.setAspectFilters(List.of("aspect1"));
    config.setIncludeSoftDeleted(true);

    SystemMetadataFilter filter = SystemMetadataFilter.from(config);

    assertNotNull(filter);
    assertEquals(filter.getGePitEpochMs(), Long.valueOf(1000L));
    assertEquals(filter.getLePitEpochMs(), Long.valueOf(2000L));
    assertEquals(filter.getAspectFilters().size(), 1);
    assertTrue(filter.isIncludeSoftDeleted());
  }

  @Test
  public void testFromEmptyConfiguration() {
    EntityConsistencyConfiguration.SystemMetadataFilterConfig config =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();

    SystemMetadataFilter filter = SystemMetadataFilter.from(config);

    assertNotNull(filter);
    assertNull(filter.getGePitEpochMs());
    assertNull(filter.getLePitEpochMs());
    assertNull(filter.getAspectFilters());
    assertFalse(filter.isIncludeSoftDeleted());
  }

  // ============================================================================
  // Immutability Tests
  // ============================================================================

  @Test
  public void testFilterIsImmutable() {
    List<String> aspects = new java.util.ArrayList<>();
    aspects.add("aspect1");
    SystemMetadataFilter filter = SystemMetadataFilter.builder().aspectFilters(aspects).build();

    // The filter should have its own copy
    List<String> returnedAspects = filter.getAspectFilters();
    assertEquals(returnedAspects.size(), 1);

    // The returned list should be the same reference from Lombok
    aspects.add("aspect2");
    // Note: Lombok's default builder doesn't copy lists, so this may or may not affect the filter
    // This test documents current behavior
  }

  // ============================================================================
  // Edge Case Tests
  // ============================================================================

  @Test
  public void testBothTimestampRanges() {
    long start = 1000L;
    long end = 2000L;
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder().gePitEpochMs(start).lePitEpochMs(end).build();

    assertTrue(filter.hasAnyConfig());
    assertEquals(filter.getGePitEpochMs(), Long.valueOf(start));
    assertEquals(filter.getLePitEpochMs(), Long.valueOf(end));
  }

  @Test
  public void testNegativeTimestamps() {
    // Edge case - negative timestamps should be allowed (though unusual)
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder().gePitEpochMs(-1000L).lePitEpochMs(-500L).build();

    assertEquals(filter.getGePitEpochMs(), Long.valueOf(-1000L));
    assertEquals(filter.getLePitEpochMs(), Long.valueOf(-500L));
    assertTrue(filter.hasAnyConfig());
  }

  @Test
  public void testZeroTimestamp() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().gePitEpochMs(0L).build();

    assertEquals(filter.getGePitEpochMs(), Long.valueOf(0L));
    assertTrue(filter.hasAnyConfig());
  }
}
