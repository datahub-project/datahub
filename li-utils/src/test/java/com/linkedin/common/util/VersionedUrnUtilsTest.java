/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.common.util;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.urn.VersionedUrnUtils;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.testng.annotations.Test;

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
