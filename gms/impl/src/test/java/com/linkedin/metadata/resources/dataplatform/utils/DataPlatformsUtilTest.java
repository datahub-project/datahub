package com.linkedin.metadata.resources.dataplatform.utils;

import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DataPlatformsUtilTest {

  @Test
  public void testGet() {
    String platformName = "hdfs";
    DataPlatformInfo platform = DataPlatformsUtil.get(platformName).orElse(null);
    assertNotNull(platform, platformName);
    assertEquals(platform.getName(), platformName, platformName);
    assertEquals(platform.getType(), PlatformType.FILE_SYSTEM, platformName);

    platformName = "fake";
    platform = DataPlatformsUtil.get(platformName).orElse(null);
    assertNull(platform, platformName);
  }

  @Test
  public void testGetPlatformType() {
    assertPlatformType("ambry", PlatformType.OBJECT_STORE);
    assertPlatformType("couchbase", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("dalids", PlatformType.FILE_SYSTEM);
    assertPlatformType("espresso", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("external", PlatformType.OTHERS);
    assertPlatformType("followfeed", PlatformType.OBJECT_STORE);
    assertPlatformType("hdfs", PlatformType.FILE_SYSTEM);
    assertPlatformType("hive", PlatformType.FILE_SYSTEM);
    assertPlatformType("kafka", PlatformType.MESSAGE_BROKER);
    assertPlatformType("kafka-lc", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("mongo", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("mysql", PlatformType.RELATIONAL_DB);
    assertPlatformType("oracle", PlatformType.RELATIONAL_DB);
    assertPlatformType("pinot", PlatformType.OLAP_DATASTORE);
    assertPlatformType("presto", PlatformType.QUERY_ENGINE);
    assertPlatformType("seas-cloud", PlatformType.SEARCH_ENGINE);
    assertPlatformType("seas-deployed", PlatformType.SEARCH_ENGINE);
    assertPlatformType("seas-hdfs", PlatformType.FILE_SYSTEM);
    assertPlatformType("teradata", PlatformType.RELATIONAL_DB);
    assertPlatformType("ump", PlatformType.FILE_SYSTEM);
    assertPlatformType("vector", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("venice", PlatformType.KEY_VALUE_STORE);
    assertPlatformType("voldemort", PlatformType.KEY_VALUE_STORE);
  }

  private void assertPlatformType(String name, PlatformType type) {
    DataPlatformInfo platform = DataPlatformsUtil.get(name).orElse(null);
    assertNotNull(platform, name);
    assertEquals(platform.getName(), name, name);
    assertEquals(platform.getType(), type, name);
  }

  @Test
  public void testIsValidPlatform() {
    String platformName = "hdfs";
    boolean validDataPlatform = DataPlatformsUtil.isValidDataPlatform(platformName);
    assertTrue(validDataPlatform);

    platformName = "fake";
    validDataPlatform = DataPlatformsUtil.isValidDataPlatform(platformName);
    assertFalse(validDataPlatform, platformName);
  }

  @Test
  public void testGetPlatformDelimiter() {
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("ambry").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("couchbase").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("dalids").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("espresso").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("external").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("followfeed").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("hdfs").get(), "/");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("hive").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("kafka").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("kafka-lc").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("mongo").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("mysql").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("oracle").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("pinot").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("presto").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("seas-cloud").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("seas-deployed").get(), "/");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("seas-hdfs").get(), "/");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("teradata").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("ump").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("vector").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("venice").get(), ".");
    assertEquals(DataPlatformsUtil.getPlatformDelimiter("voldemort").get(), ".");
    assertFalse(DataPlatformsUtil.getPlatformDelimiter("fake").isPresent());
  }
}
