package com.linkedin.datahub.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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
   * Split WhereHows dataset URN into two parts: platform + dataset name
   * Also replace '/' with '.'getObjectArrayListBy in dataset name other than hdfs datasets
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

}
