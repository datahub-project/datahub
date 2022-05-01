package com.linkedin.common.util;

import com.linkedin.common.urn.VersionedUrnUtils;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class VersionedUrnUtilsTest {

  private static final String SCHEMA_METADATA = "schemaMetadata";
  private static final String DATASET_KEY = "datasetKey";
  private static final String SOME_ASPECT = "someAspect";

  @Test
  public void testVersionStampConstructConvert() {
    SortedMap<String, Long> sortedMap = new TreeMap<>(Comparator.naturalOrder());
    sortedMap.put(SCHEMA_METADATA, 15L);
    sortedMap.put(DATASET_KEY, 0L);
    sortedMap.put(SOME_ASPECT, 111231242456L);
    String versionStamp = VersionedUrnUtils.constructVersionStamp(sortedMap);

    Map<String, Long> versionStampMap = VersionedUrnUtils.convertVersionStamp(versionStamp);
    assertEquals(versionStampMap.get(SCHEMA_METADATA), sortedMap.get(SCHEMA_METADATA));
    assertEquals(versionStampMap.get(DATASET_KEY), sortedMap.get(DATASET_KEY));
    assertEquals(versionStampMap.get(SOME_ASPECT), sortedMap.get(SOME_ASPECT));
  }
}
