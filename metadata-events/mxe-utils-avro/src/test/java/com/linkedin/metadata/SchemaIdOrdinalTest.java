package com.linkedin.metadata;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class SchemaIdOrdinalTest {

  @Test
  public void testSchemaIdConstants() {
    // Test that all schema ID constants have the expected values
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId(), 0);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1.getSchemaId(), 1);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId(), 2);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId(), 3);
    assertEquals(SchemaIdOrdinal.PLATFORM_EVENT.getSchemaId(), 4);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId(), 5);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1.getSchemaId(), 6);
    assertEquals(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1.getSchemaId(), 7);
    assertEquals(SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId(), 8);

    // Test breaking changes from V1
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId(), 9);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL.getSchemaId(), 10);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId(), 11);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId(), 12);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId(), 13);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT.getSchemaId(), 14);
    assertEquals(SchemaIdOrdinal.METADATA_AUDIT_EVENT.getSchemaId(), 15);
  }

  @Test
  public void testGetSchemaId() {
    // Test that getSchemaId() returns the correct value for each ordinal
    for (SchemaIdOrdinal ordinal : SchemaIdOrdinal.values()) {
      int expectedId = getExpectedSchemaId(ordinal);
      assertEquals(
          ordinal.getSchemaId(),
          expectedId,
          "SchemaIdOrdinal." + ordinal.name() + " should have schema ID " + expectedId);
    }
  }

  @Test
  public void testFromSchemaId() {
    // Test that fromSchemaId() returns the correct ordinal for valid schema IDs
    assertEquals(SchemaIdOrdinal.fromSchemaId(0), SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1);
    assertEquals(
        SchemaIdOrdinal.fromSchemaId(1), SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1);
    assertEquals(SchemaIdOrdinal.fromSchemaId(2), SchemaIdOrdinal.METADATA_CHANGE_LOG_V1);
    assertEquals(
        SchemaIdOrdinal.fromSchemaId(3), SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1);
    assertEquals(SchemaIdOrdinal.fromSchemaId(4), SchemaIdOrdinal.PLATFORM_EVENT);
    assertEquals(SchemaIdOrdinal.fromSchemaId(5), SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1);
    assertEquals(SchemaIdOrdinal.fromSchemaId(6), SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1);
    assertEquals(SchemaIdOrdinal.fromSchemaId(7), SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1);
    assertEquals(SchemaIdOrdinal.fromSchemaId(8), SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT);
    assertEquals(SchemaIdOrdinal.fromSchemaId(9), SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL);
    assertEquals(SchemaIdOrdinal.fromSchemaId(10), SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL);
    assertEquals(SchemaIdOrdinal.fromSchemaId(11), SchemaIdOrdinal.METADATA_CHANGE_LOG);
    assertEquals(SchemaIdOrdinal.fromSchemaId(12), SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES);
    assertEquals(SchemaIdOrdinal.fromSchemaId(13), SchemaIdOrdinal.METADATA_CHANGE_EVENT);
    assertEquals(SchemaIdOrdinal.fromSchemaId(14), SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT);
    assertEquals(SchemaIdOrdinal.fromSchemaId(15), SchemaIdOrdinal.METADATA_AUDIT_EVENT);
  }

  @Test
  public void testFromSchemaIdWithInvalidId() {
    // Test that fromSchemaId() returns null for invalid schema IDs
    assertNull(SchemaIdOrdinal.fromSchemaId(-1));
    assertNull(SchemaIdOrdinal.fromSchemaId(16));
    assertNull(SchemaIdOrdinal.fromSchemaId(999));
    assertNull(SchemaIdOrdinal.fromSchemaId(Integer.MAX_VALUE));
    assertNull(SchemaIdOrdinal.fromSchemaId(Integer.MIN_VALUE));
  }

  @Test
  public void testIsValidSchemaId() {
    // Test that isValidSchemaId() returns true for valid schema IDs
    assertTrue(SchemaIdOrdinal.isValidSchemaId(0));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(1));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(2));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(3));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(4));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(5));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(6));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(7));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(8));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(9));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(10));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(11));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(12));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(13));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(14));
    assertTrue(SchemaIdOrdinal.isValidSchemaId(15));

    // Test that isValidSchemaId() returns false for invalid schema IDs
    assertFalse(SchemaIdOrdinal.isValidSchemaId(-1));
    assertFalse(SchemaIdOrdinal.isValidSchemaId(16));
    assertFalse(SchemaIdOrdinal.isValidSchemaId(999));
    assertFalse(SchemaIdOrdinal.isValidSchemaId(Integer.MAX_VALUE));
    assertFalse(SchemaIdOrdinal.isValidSchemaId(Integer.MIN_VALUE));
  }

  @Test
  public void testGetAllSchemaIds() {
    // Test that getAllSchemaIds() returns all valid schema IDs in order
    int[] allSchemaIds = SchemaIdOrdinal.getAllSchemaIds();

    assertNotNull(allSchemaIds);
    assertEquals(allSchemaIds.length, SchemaIdOrdinal.values().length);

    // Verify the IDs are in ascending order
    for (int i = 0; i < allSchemaIds.length - 1; i++) {
      assertTrue(allSchemaIds[i] < allSchemaIds[i + 1], "Schema IDs should be in ascending order");
    }

    // Verify all expected IDs are present
    assertTrue(contains(allSchemaIds, 0));
    assertTrue(contains(allSchemaIds, 1));
    assertTrue(contains(allSchemaIds, 2));
    assertTrue(contains(allSchemaIds, 3));
    assertTrue(contains(allSchemaIds, 4));
    assertTrue(contains(allSchemaIds, 5));
    assertTrue(contains(allSchemaIds, 6));
    assertTrue(contains(allSchemaIds, 7));
    assertTrue(contains(allSchemaIds, 8));
    assertTrue(contains(allSchemaIds, 9));
    assertTrue(contains(allSchemaIds, 10));
    assertTrue(contains(allSchemaIds, 11));
    assertTrue(contains(allSchemaIds, 12));
    assertTrue(contains(allSchemaIds, 13));
    assertTrue(contains(allSchemaIds, 14));
    assertTrue(contains(allSchemaIds, 15));
  }

  @Test
  public void testValues() {
    // Test that values() returns all expected ordinals
    SchemaIdOrdinal[] ordinals = SchemaIdOrdinal.values();

    assertNotNull(ordinals);
    assertEquals(ordinals.length, 16, "Should have exactly 16 schema ID ordinals");

    // Verify all expected ordinals are present
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_LOG_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.PLATFORM_EVENT));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1));
    assertTrue(contains(ordinals, SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL));
    assertTrue(contains(ordinals, SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_LOG));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_CHANGE_EVENT));
    assertTrue(contains(ordinals, SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT));
    assertTrue(contains(ordinals, SchemaIdOrdinal.METADATA_AUDIT_EVENT));
  }

  @Test
  public void testSchemaIdUniqueness() {
    // Test that all schema IDs are unique
    int[] allSchemaIds = SchemaIdOrdinal.getAllSchemaIds();

    for (int i = 0; i < allSchemaIds.length; i++) {
      for (int j = i + 1; j < allSchemaIds.length; j++) {
        assertNotEquals(
            allSchemaIds[i],
            allSchemaIds[j],
            "Schema IDs should be unique: " + allSchemaIds[i] + " and " + allSchemaIds[j]);
      }
    }
  }

  @Test
  public void testV1SchemaIds() {
    // Test that V1 schema IDs are in the expected range (0-7)
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1.getSchemaId() <= 7);
    assertTrue(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1.getSchemaId() >= 0);
    assertTrue(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1.getSchemaId() <= 7);
  }

  @Test
  public void testCurrentSchemaIds() {
    // Test that current schema IDs are in the expected range (9-15)
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT.getSchemaId() <= 15);
    assertTrue(SchemaIdOrdinal.METADATA_AUDIT_EVENT.getSchemaId() >= 9);
    assertTrue(SchemaIdOrdinal.METADATA_AUDIT_EVENT.getSchemaId() <= 15);
  }

  @Test
  public void testSpecialCaseSchemaIds() {
    // Test special cases
    assertEquals(
        SchemaIdOrdinal.PLATFORM_EVENT.getSchemaId(), 4, "PLATFORM_EVENT should have schema ID 4");
    assertEquals(
        SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId(),
        8,
        "DATAHUB_UPGRADE_HISTORY_EVENT should have schema ID 8");
  }

  @Test
  public void testRoundTripConsistency() {
    // Test that fromSchemaId(getSchemaId()) returns the same ordinal
    for (SchemaIdOrdinal ordinal : SchemaIdOrdinal.values()) {
      int schemaId = ordinal.getSchemaId();
      SchemaIdOrdinal roundTrip = SchemaIdOrdinal.fromSchemaId(schemaId);
      assertEquals(
          roundTrip, ordinal, "Round trip should return the same ordinal: " + ordinal.name());
    }
  }

  @Test
  public void testSchemaIdValidationConsistency() {
    // Test that isValidSchemaId() and fromSchemaId() are consistent
    for (int i = -5; i < 20; i++) {
      boolean isValid = SchemaIdOrdinal.isValidSchemaId(i);
      SchemaIdOrdinal ordinal = SchemaIdOrdinal.fromSchemaId(i);

      if (isValid) {
        assertNotNull(ordinal, "Valid schema ID " + i + " should return a non-null ordinal");
        assertEquals(ordinal.getSchemaId(), i, "Ordinal should have the expected schema ID");
      } else {
        assertNull(ordinal, "Invalid schema ID " + i + " should return null");
      }
    }
  }

  // Helper methods
  private int getExpectedSchemaId(SchemaIdOrdinal ordinal) {
    switch (ordinal) {
      case METADATA_CHANGE_PROPOSAL_V1:
        return 0;
      case FAILED_METADATA_CHANGE_PROPOSAL_V1:
        return 1;
      case METADATA_CHANGE_LOG_V1:
        return 2;
      case METADATA_CHANGE_LOG_TIMESERIES_V1:
        return 3;
      case PLATFORM_EVENT:
        return 4;
      case METADATA_CHANGE_EVENT_V1:
        return 5;
      case FAILED_METADATA_CHANGE_EVENT_V1:
        return 6;
      case METADATA_AUDIT_EVENT_V1:
        return 7;
      case DATAHUB_UPGRADE_HISTORY_EVENT:
        return 8;
      case METADATA_CHANGE_PROPOSAL:
        return 9;
      case FAILED_METADATA_CHANGE_PROPOSAL:
        return 10;
      case METADATA_CHANGE_LOG:
        return 11;
      case METADATA_CHANGE_LOG_TIMESERIES:
        return 12;
      case METADATA_CHANGE_EVENT:
        return 13;
      case FAILED_METADATA_CHANGE_EVENT:
        return 14;
      case METADATA_AUDIT_EVENT:
        return 15;
      default:
        throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
    }
  }

  private boolean contains(int[] array, int value) {
    for (int item : array) {
      if (item == value) return true;
    }
    return false;
  }

  private boolean contains(SchemaIdOrdinal[] array, SchemaIdOrdinal value) {
    for (SchemaIdOrdinal item : array) {
      if (item == value) return true;
    }
    return false;
  }
}
