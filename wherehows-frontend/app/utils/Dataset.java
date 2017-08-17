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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.cache.Cache;


public class Dataset {
  public static final String URNIDMAPKey = "impactUrnIDMap";
  private static Cache currentCache = null;

  public static Cache getChacheInstance() {
    if (currentCache == null) {
      currentCache = new Cache();
    }
    return currentCache;
  }

  public static JsonNode addDatasetIDtoImpactAnalysisResult(JsonNode impacts) {
    if (impacts == null || (!impacts.isArray())) {
      return impacts;
    }

    try {
      Map<String, Integer> urnIDMap = new HashMap<String, Integer>();
      if (getChacheInstance().get("URNIDMAPKey") != null) {
        urnIDMap = (Map<String, Integer>) getChacheInstance().get("URNIDMAPKey");
      } else {
        FileReader fr = new FileReader("/var/tmp/wherehows/resource/dataset_urn_list.txt");
        BufferedReader br = new BufferedReader(fr);
        String line;

        while ((line = br.readLine()) != null) {
          String record = line.substring(0, line.length());
          String[] values = record.split("\t");
          if (values != null && values.length == 2) {
            String urn = values[0];
            Integer id = Integer.parseInt(values[1]);
            if (StringUtils.isNotBlank(urn) && id != null && id != 0) {
              urnIDMap.put(urn, id);
            }
          }
        }
        if (urnIDMap != null && urnIDMap.size() > 0) {
          getChacheInstance().set(URNIDMAPKey, urnIDMap);
        }
      }

      if (urnIDMap != null && urnIDMap.size() > 0) {
        Iterator<JsonNode> arrayIterator = impacts.elements();
        if (arrayIterator != null) {
          while (arrayIterator.hasNext()) {
            JsonNode node = arrayIterator.next();
            if (node.isContainerNode() && node.has("urn")) {
              String urn = node.get("urn").asText();
              ObjectNode objectNode = (ObjectNode) node;
              int index = urn.lastIndexOf("/");
              String name = urn.substring(index + 1);
              objectNode.put("name", name);
              if (urnIDMap.containsKey(urn)) {
                Integer id = urnIDMap.get(urn);
                objectNode.put("id", id);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      Logger.error(e.getMessage());
    }
    return impacts;
  }
}