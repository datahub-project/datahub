/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.common.urn;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.StringUtils;

public class VersionedUrnUtils {

  private VersionedUrnUtils() {}

  public static Map<String, Long> convertVersionStamp(String versionStamp) {
    Map<String, Long> aspectVersionMap = new HashMap<>();
    if (StringUtils.isBlank(versionStamp)) {
      return aspectVersionMap;
    }
    String[] aspectNameVersionPairs = versionStamp.split(";");
    for (String pair : aspectNameVersionPairs) {
      String[] tokens = pair.split(":");
      if (tokens.length != 2) {
        throw new IllegalArgumentException(
            "Invalid version stamp cannot be parsed: " + versionStamp);
      }
      try {
        aspectVersionMap.put(tokens[0], Long.valueOf(tokens[1]));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid value for aspect version: " + tokens[1]);
      }
    }

    return aspectVersionMap;
  }

  public static String constructVersionStamp(SortedMap<String, Long> versionStampMap) {
    StringBuilder versionStamp =
        versionStampMap.entrySet().stream()
            .collect(
                StringBuilder::new,
                (builder, entry) ->
                    builder.append(entry.getKey()).append(":").append(entry.getValue()).append(";"),
                StringBuilder::append);
    // trim off last ;
    return versionStamp.substring(0, versionStamp.length() - 1);
  }
}
