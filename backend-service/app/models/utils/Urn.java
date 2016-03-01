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
package models.utils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import play.Logger;


/**
 * Urn class used for urn convertion
 * Created by zsun on 1/15/15.
 */
public class Urn {
  public String urnString;
  public String storageType;
  public String schemaName;
  public String abstractObjectName;

  static final String[] stoList = new String[] {"teradata", "hdfs"};
  static final Set<String> storageTypes = new HashSet<String>(Arrays.asList(stoList));

  public Urn(String urnString) {
    this.urnString = urnString;
    String[] splitResult = urnString.split(":///");
    storageType = splitResult[0].toLowerCase();
    Logger.debug(urnString);
    switch (storageType) {
            /* example: hdfs://data/tracking/PageViewEvent -> 'hdfs', '', 'data/tracking/PageViewEvent' */
      case "hdfs": abstractObjectName = "/" + splitResult[1];
        schemaName = "";
        break;
            /* example: teradata://dwh/dwh_dim/domain_name -> 'teradata', 'dwh/dwh_dim', 'domain_name' */
      case "teradata": String[] split2 = splitResult[1].split("/");
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

  public Urn(String storageType, String schemaName, String abstractObjectName) {
    this.storageType = storageType.toLowerCase();
    if (schemaName != null)
      this.schemaName = schemaName.toLowerCase();
    this.abstractObjectName = abstractObjectName;
    switch (this.storageType) {
      case "teradata" : urnString = "teradata:///" + schemaName + "/" + abstractObjectName;
        break;
      default: String trimName = abstractObjectName.startsWith("/") ? abstractObjectName.substring(1) : abstractObjectName;
        urnString = this.storageType + ":///" + trimName;
    }
  }

  public static boolean validateUrn(String urnString) {

    String[] splitResult = urnString.split(":///");
    if (storageTypes.contains(splitResult[0]) && splitResult.length > 1)
      return true;
    return false;
  }
}
