package io.datahubproject.event;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class TopicAllowListTest {

  @Test
  public void testExactMatchAllowsKnownTopic() {
    TopicAllowList list = new TopicAllowList("PlatformEvent_v1,MetadataChangeLog_Versioned_v1");
    assertTrue(list.isAllowed("PlatformEvent_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Versioned_v1"));
  }

  @Test
  public void testExactMatchRejectsUnknownTopic() {
    TopicAllowList list = new TopicAllowList("PlatformEvent_v1");
    assertFalse(list.isAllowed("SomeOtherTopic"));
  }

  @Test
  public void testPrefixMatchAllowsMatchingTopics() {
    TopicAllowList list = new TopicAllowList("MetadataChangeLog_*");
    assertTrue(list.isAllowed("MetadataChangeLog_Versioned_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Timeseries_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Custom"));
  }

  @Test
  public void testPrefixMatchRejectsNonMatchingTopics() {
    TopicAllowList list = new TopicAllowList("MetadataChangeLog_*");
    assertFalse(list.isAllowed("PlatformEvent_v1"));
    assertFalse(list.isAllowed("Meta"));
  }

  @Test
  public void testDefaultThreeTopics() {
    TopicAllowList list =
        new TopicAllowList(
            "PlatformEvent_v1,MetadataChangeLog_Versioned_v1,MetadataChangeLog_Timeseries_v1");
    assertTrue(list.isAllowed("PlatformEvent_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Versioned_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Timeseries_v1"));
    assertFalse(list.isAllowed("CustomTopic"));
    assertFalse(list.isAllowed("DataHubUsageEvent_v1"));
  }

  @Test
  public void testWhitespaceTrimming() {
    TopicAllowList list = new TopicAllowList("  PlatformEvent_v1 , MetadataChangeLog_* ");
    assertTrue(list.isAllowed("PlatformEvent_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Versioned_v1"));
  }

  @Test
  public void testMixedExactAndPrefix() {
    TopicAllowList list = new TopicAllowList("PlatformEvent_v1,custom.*");
    assertTrue(list.isAllowed("PlatformEvent_v1"));
    assertTrue(list.isAllowed("custom.my-company.events"));
    assertTrue(list.isAllowed("custom.other"));
    assertFalse(list.isAllowed("MetadataChangeLog_Versioned_v1"));
  }

  @Test
  public void testEmptyEntriesIgnored() {
    TopicAllowList list = new TopicAllowList("PlatformEvent_v1,,, ,MetadataChangeLog_*");
    assertTrue(list.isAllowed("PlatformEvent_v1"));
    assertTrue(list.isAllowed("MetadataChangeLog_Versioned_v1"));
    assertFalse(list.isAllowed("Other"));
  }
}
