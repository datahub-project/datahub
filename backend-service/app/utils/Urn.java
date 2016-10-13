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
package utils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import play.Logger;


/**
 * Urn class used for urn conversion
 * Created by zsun on 1/15/15.
 */
public class Urn {
  public String urnString;
  public String datasetType;
  public String schemaName;
  public String abstractObjectName;

  static final String[] stoList = new String[] {"teradata", "hdfs", "hive", "dalids", "oracle", "mysql", "pinot"};
  static final Set<String> datasetTypes = new HashSet<String>(Arrays.asList(stoList));

  /**
   * Urn can contain 3 parts
   *      (1)           (2)           (3)
   * dataset_type://cluster:port/parent/name
   * the 2nd part is only used to identify deployed dataset instance
   * for dataset definition, we only use part (1) + (3)
   */
  public Urn(String urnString) {
    this.urnString = urnString;
    String[] splitResult = urnString.split(":///");
    datasetType = splitResult[0].toLowerCase();
    Logger.debug(urnString);
    switch (datasetType) {
      /* example: hdfs:///data/tracking/PageViewEvent -> 'hdfs', '', 'data/tracking/PageViewEvent' */
      case "hdfs": abstractObjectName = "/" + splitResult[1];
        schemaName = "";
        break;
      /* example: teradata:///dwh_dim/dim_table_name -> 'teradata', 'dwh_dim', 'dim_table_name'
      *           hive:///db_name/table_name -> 'hive', 'db_name', 'table_name'
      * */
      case "teradata":
      case "oracle":
      case "mysql":
      case "espresso":
      case "pinot":
      case "hive":
      case "dalids":
        String[] split2 = splitResult[1].split("/");
        abstractObjectName = split2[split2.length-1];
        StringBuffer sb = new StringBuffer();
        if (split2.length > 1) {
          for (int i = 0; i < split2.length - 1; i++) {
            sb.append(split2[i]);
            sb.append("/");
          }
          sb.deleteCharAt(sb.length() - 1);
        }
        schemaName = sb.toString();
        break;
      default: abstractObjectName = splitResult[1];
    }
  }

  public Urn(String datasetType, String schemaName, String abstractObjectName) {
    this.datasetType = datasetType.toLowerCase();
    if (schemaName != null)
      this.schemaName = schemaName.toLowerCase();
    this.abstractObjectName = abstractObjectName;
    switch (this.datasetType) {
      case "teradata":
      case "oracle":
      case "mysql":
      case "espresso":
      case "pinot":
      case "hive":
      case "dalids":
        urnString = this.datasetType + ":///" + schemaName + "/" + abstractObjectName;
        break;
      default: String trimName = abstractObjectName.startsWith("/") ? abstractObjectName.substring(1) : abstractObjectName;
        urnString = this.datasetType + ":///" + trimName;
    }
  }

  public static boolean validateUrn(String urnString) {

    String[] splitResult = urnString.split(":///");
    if ((datasetTypes.contains(splitResult[0]) || splitResult[0].matches("\\w+")) &&
        splitResult.length > 1)
      return true;
    return false;
  }
}
