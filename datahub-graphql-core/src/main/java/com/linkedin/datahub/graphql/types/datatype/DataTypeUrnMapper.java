/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.datatype;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.StdDataType;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataTypeUrnMapper {

  static final Map<StdDataType, String> DATA_TYPE_ENUM_TO_URN =
      ImmutableMap.<StdDataType, String>builder()
          .put(StdDataType.STRING, "urn:li:dataType:datahub.string")
          .put(StdDataType.NUMBER, "urn:li:dataType:datahub.number")
          .put(StdDataType.URN, "urn:li:dataType:datahub.urn")
          .put(StdDataType.RICH_TEXT, "urn:li:dataType:datahub.rich_text")
          .put(StdDataType.DATE, "urn:li:dataType:datahub.date")
          .build();

  private static final Map<String, StdDataType> URN_TO_DATA_TYPE_ENUM =
      DATA_TYPE_ENUM_TO_URN.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

  private DataTypeUrnMapper() {}

  public static StdDataType getType(String dataTypeUrn) {
    if (!URN_TO_DATA_TYPE_ENUM.containsKey(dataTypeUrn)) {
      return StdDataType.OTHER;
    }
    return URN_TO_DATA_TYPE_ENUM.get(dataTypeUrn);
  }

  @Nonnull
  public static String getUrn(StdDataType dataType) {
    if (!DATA_TYPE_ENUM_TO_URN.containsKey(dataType)) {
      throw new IllegalArgumentException("Unknown data type: " + dataType);
    }
    return DATA_TYPE_ENUM_TO_URN.get(dataType);
  }
}
