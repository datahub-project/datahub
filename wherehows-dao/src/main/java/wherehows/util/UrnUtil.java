/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.util;

import com.linkedin.events.metadata.DatasetIdentifier;
import org.apache.commons.lang3.StringUtils;


public class UrnUtil {

  private UrnUtil() {
  }

  /**
   * Get the entity (last part) of an URN, e.g. urn:li:user:abc -> abc
   * @param urn colon separated URN String
   * @return entity String
   */
  public static String getUrnEntity(String urn) {
    return urn == null ? null : urn.substring(urn.lastIndexOf(':') + 1);
  }

  /**
   * Construct WhereHows dataset URN from DatasetIdentifier
   * Replace '.' with '/' for dataset name in Espresso, Oracle, Dalids, Hive and Teradata
   * It is the reverse of splitWhUrn
   * @param datasetIdentifier DatasetIdentifier
   * @return String WH URN
   */
  public static String toWhDatasetUrn(DatasetIdentifier datasetIdentifier) {
    String platform = getUrnEntity(datasetIdentifier.dataPlatformUrn.toString());
    String name = datasetIdentifier.nativeName.toString();

    if ("espresso".equalsIgnoreCase(platform) || "oracle".equalsIgnoreCase(platform) || "dalids".equalsIgnoreCase(platform)
        || "hive".equalsIgnoreCase(platform) || "teradata".equalsIgnoreCase(platform)) {
      name = name.replace(".", "/");
    }

    if ("hdfs".equalsIgnoreCase(platform) && name.startsWith("/")) {
      name = name.substring(1);
    }

    return platform + ":///" + name;
  }

  /**
   * Split WH dataset urn into platform, prefix, parent name, dataset name
   * For platform:///abc ->  prefix '', parent '', dataset 'abc'
   * For hdfs:///a/b/c/d  ->  prefix '/a/b', parent '/a/b/c', dataset 'd'
   * For platform:///a/b  ->  prefix '/a', parent 'a', dataset 'b'
   * @param urn String
   * @return String[platform, prefix, parentName, datasetName]
   */
  public static String[] splitWhDatasetUrn(String urn) {
    String platform = urn.split(":///")[0];
    String fullname = urn.split(":///")[1];

    String prefix;
    String parent;
    String datasetName;
    if (fullname.indexOf('/') < 0) {
      prefix = "";
      parent = "";
      datasetName = fullname;
    } else if ("hdfs".equalsIgnoreCase(platform)) {
      parent = "/" + fullname.substring(0, fullname.lastIndexOf('/'));
      prefix = "/" + fullname.substring(0, StringUtils.ordinalIndexOf(fullname, "/", 2));
      datasetName = fullname.substring(fullname.lastIndexOf('/') + 1);
    } else {
      parent = fullname.substring(0, fullname.lastIndexOf('/'));
      prefix = "/" + parent;
      datasetName = fullname.substring(fullname.lastIndexOf('/') + 1);
    }

    return new String[]{platform, prefix, parent, datasetName};
  }

  /**
   * Return the first non-null object from multiple objects
   * @param objs objects
   * @return first non-null object, or null in case that all objects are null
   */
  @SafeVarargs
  public static <T> T coalesce(T... objs) {
    for (T obj : objs) {
      if (obj != null) {
        return obj;
      }
    }
    return null;
  }

  /**
   * Object to string, or null if object = null
   * @param obj Object
   * @return String or null
   */
  public static String toStringOrNull(Object obj) {
    return obj == null ? null : obj.toString();
  }

  /**
   * Trim string to be at most 'maxLength' characters, if less, return the original string.
   * @param s String
   * @param maxLength int
   * @return trimmed String
   */
  public static String trimToLength(String s, int maxLength) {
    if (s == null) {
      return null;
    }

    return s.substring(0, Math.min(s.length(), maxLength));
  }
}
