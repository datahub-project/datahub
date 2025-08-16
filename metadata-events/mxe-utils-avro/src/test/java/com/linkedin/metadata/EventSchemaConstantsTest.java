package com.linkedin.metadata;

import static org.testng.Assert.*;

import java.util.List;
import org.testng.annotations.Test;

public class EventSchemaConstantsTest {

  @Test
  public void testSchemaVersionConstants() {
    assertEquals(EventSchemaConstants.SCHEMA_VERSION_1, 1);
    assertEquals(EventSchemaConstants.SCHEMA_VERSION_2, 2);
  }

  @Test
  public void testGetLatestSchemaVersionForMetadataChangeProposal() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_2);
  }

  @Test
  public void testGetLatestSchemaVersionForMetadataChangeLog() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForMetadataChangeEvent() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForFailedMetadataChangeEvent() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForMetadataAuditEvent() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForPlatformEvent() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.PLATFORM_EVENT_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForDatahubUpgradeHistoryEvent() {
    int latestVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForUnknownSchema() {
    int latestVersion = EventSchemaConstants.getLatestSchemaVersion("UnknownSchema");
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetLatestSchemaVersionForNullSchema() {
    int latestVersion = EventSchemaConstants.getLatestSchemaVersion(null);
    assertEquals(latestVersion, EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetSupportedVersionsForMetadataChangeProposal() {
    List<Integer> versions =
        EventSchemaConstants.getSupportedVersions(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    assertEquals(versions.size(), 2);
    assertTrue(versions.contains(EventSchemaConstants.SCHEMA_VERSION_1));
    assertTrue(versions.contains(EventSchemaConstants.SCHEMA_VERSION_2));
  }

  @Test
  public void testGetSupportedVersionsForMetadataChangeLog() {
    List<Integer> versions =
        EventSchemaConstants.getSupportedVersions(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    assertEquals(versions.size(), 1);
    assertEquals(versions.get(0), EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetSupportedVersionsForUnknownSchema() {
    List<Integer> versions = EventSchemaConstants.getSupportedVersions("UnknownSchema");
    assertEquals(versions.size(), 1);
    assertEquals(versions.get(0), EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testGetSupportedVersionsForNullSchema() {
    List<Integer> versions = EventSchemaConstants.getSupportedVersions(null);
    assertEquals(versions.size(), 1);
    assertEquals(versions.get(0), EventSchemaConstants.SCHEMA_VERSION_1);
  }

  @Test
  public void testUtilityClassCannotBeInstantiated() {
    try {
      EventSchemaConstants.class.getDeclaredConstructor().newInstance();
      fail("Expected exception when trying to instantiate utility class");
    } catch (Exception e) {
      // Expected - utility class should not be instantiable
      assertTrue(e instanceof ReflectiveOperationException);
    }
  }
}
