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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class UrnUtil {

  private UrnUtil() {
  }

  /**
   * Get the entity (last part) of an URN, e.g. urn:li:user:abc -> abc
   * If input null, return null
   * @param urn colon separated URN String
   * @return entity String
   */
  public static String getUrnEntity(@Nullable String urn) {
    return urn == null ? null : urn.substring(urn.lastIndexOf(':') + 1);
  }

  /**
   * Construct WhereHows dataset URN from DatasetIdentifier
   * Replace '.' with '/' in dataset name
   * It is the reverse of splitWhUrn
   * @param datasetIdentifier DatasetIdentifier
   * @return String WH URN
   */
  public static String toWhDatasetUrn(@Nonnull DatasetIdentifier datasetIdentifier) {
    String platform = getUrnEntity(datasetIdentifier.dataPlatformUrn.toString());
    String name = datasetIdentifier.nativeName.toString();

    if ("hdfs".equalsIgnoreCase(platform)) {
      if (name.startsWith("/")) {
        name = name.substring(1);
      }
    } else {
      name = name.replace(".", "/");
    }

    return platform + ":///" + name;
  }

  /**
   * Split WhereHows dataset URN into two parts: platform + dataset name
   * Also replace '/' with '.' in dataset name other than hdfs datasets
   * E.g. oracle:///abc/def > [oracle, abc.def]
   * @param urn String WhereHows dataset URN
   * @return String[] platform + dataset name
   */
  public static String[] splitWhUrn(@Nonnull String urn) {
    if (urn == null) {
      throw new IllegalArgumentException("URN is null");
    }

    final String[] parts = urn.split(":///"); // [platform, dataset name]

    if ("hdfs".equalsIgnoreCase(parts[0])) {
      parts[1] = "/" + parts[1];
    } else {
      parts[1] = parts[1].replace("/", ".");
    }

    return parts;
  }

  /**
   * Parse WH dataset urn into platform, prefix, parent name, dataset name
   * For platform:///abc ->  prefix '', parent '', dataset 'abc'
   * For hdfs:///a/b/c/d  ->  prefix '/a/b', parent '/a/b/c', dataset 'd'
   * For platform:///a/b  ->  prefix '/a', parent 'a', dataset 'b'
   * @param urn String
   * @return String[platform, prefix, parentName, datasetName]
   */
  public static String[] parseWhDatasetUrn(@Nonnull String urn) {
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
  public static String toStringOrNull(@Nullable Object obj) {
    return obj == null ? null : obj.toString();
  }

  /**
   * Trim string to be at most 'maxLength' characters, if less, return the original string.
   * If input null, return null.
   * @param s String
   * @param maxLength int
   * @return trimmed String
   */
  public static String trimToLength(@Nullable String s, int maxLength) {
    if (s == null) {
      return null;
    }

    return s.substring(0, Math.min(s.length(), maxLength));
  }

  /**
   * Get the enum name or return default value if enum is null
   * @param e Enum
   * @param defaultValue String
   * @return String
   */
  public static String enumNameOrDefault(@Nullable Enum e, @Nullable String defaultValue) {
    return e == null ? defaultValue : e.name();
  }
}
