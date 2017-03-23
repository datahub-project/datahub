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
package wherehows.restli.util;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public class UrnUtil {

  /**
   * Transform platform name to DataPlatformUrn
   * @param platformName String
   * @return DataPlatformUrn
   * @throws URISyntaxException
   */
  public static DataPlatformUrn toDataPlatformUrn(String platformName) throws URISyntaxException {
    return DataPlatformUrn.deserialize("urn:li:dataPlatform:" + platformName);
  }

  /**
   * Transform platform name, dataset name and origin fabric into DatasetUrn
   * @param platformName String
   * @param datasetName String
   * @param origin String
   * @return DatasetUrn
   * @throws URISyntaxException
   */
  public static DatasetUrn toDatasetUrn(String platformName, String datasetName, String origin)
      throws URISyntaxException {
    return DatasetUrn.createFromUrn(
        Urn.createFromTuple("dataset", toDataPlatformUrn(platformName), datasetName, toFabricType(origin)));
  }

  /**
   * Transform fabric string into FabricType enum
   * @param fabric String
   * @return FabricType
   */
  public static FabricType toFabricType(String fabric) {
    switch (fabric.toUpperCase()) {
      case "PROD":
        return FabricType.PROD;
      case "CORP":
        return FabricType.CORP;
      case "EI":
        return FabricType.EI;
      case "DEV":
        return FabricType.DEV;
      default:
        return FabricType.$UNKNOWN;
    }
  }

  /**
   * Split WhereHows dataset URN into two parts: platform + dataset name
   * @param urn String WhereHows dataset URN
   * @return String[] platform + dataset name
   */
  public static String[] splitWhUrn(String urn) {
    int index = urn.indexOf(":///");
    String fabric = urn.substring(0, index);
    String dataset = urn.substring(index + 4);

    // for espresso, change '/' back to '.'
    if (fabric.equalsIgnoreCase("espresso")) {
      dataset = dataset.replace("/", ".");
    }
    return new String[]{fabric, dataset};
  }
}
