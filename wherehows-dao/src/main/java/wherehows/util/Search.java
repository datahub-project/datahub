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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;


@Slf4j
public class Search {

  private Search() {
  }

  private static final ObjectMapper _OM = new ObjectMapper();

  private final static String DATASET_CATEGORY = "datasets";
  private final static String METRIC_CATEGORY = "metrics";
  private final static String COMMENT_CATEGORY = "comments";
  private final static String FLOW_CATEGORY = "flows";
  private final static String JOB_CATEGORY = "jobs";

  private static final String WHZ_ELASTICSEARCH_DATASET_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_DATASET_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_METRIC_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_METRIC_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_FLOW_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_FLOW_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_COMMENT_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_COMMENT_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_SUGGESTER_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_SUGGESTER_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_AUTO_COMPLETION_QUERY_FILE =
      System.getenv("WHZ_ELASTICSEARCH_AUTO_COMPLETION_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_FILTER_UNIT_FILE = System.getenv("WHZ_ELASTICSEARCH_FILTER_UNIT");
  private static final String WHZ_ELASTICSEARCH_FILTER_UNIT_FILE_FABRIC =
      System.getenv("WHZ_ELASTICSEARCH_FILTER_UNIT_FABRIC");

  public static String readJsonQueryFile(String jsonFile) {
    try {
      String contents = new String(Files.readAllBytes(Paths.get(jsonFile)));
      JsonNode json = _OM.readTree(contents);
      return json.toString();
    } catch (Exception e) {
      log.error("ReadJsonQueryFile failed. Error: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  public static ObjectNode generateElasticSearchCompletionSuggesterQuery(String field, String searchKeyword,
      int limit) {
    if (StringUtils.isBlank(searchKeyword)) {
      return null;
    }

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_AUTO_COMPLETION_QUERY_FILE);
    String query = queryTemplate.replace("$SEARCHKEYWORD", searchKeyword.toLowerCase());

    if (StringUtils.isNotBlank(field)) {
      query = query.replace("$FIELD", field.toLowerCase());
    }

    query = query.replace("$LIMIT", Integer.toString(limit));

    ObjectNode suggestNode = _OM.createObjectNode();

    try {
      ObjectNode textNode = (ObjectNode) _OM.readTree(query);
      suggestNode.putPOJO("suggest", textNode);
    } catch (Exception e) {
      log.error("suggest Exception = " + e.getMessage());
    }

    log.info("completionSuggesterQuery suggestNode is " + suggestNode.toString());
    return suggestNode;
  }

  public static ObjectNode generateElasticSearchPhraseSuggesterQuery(String field, String searchKeyword) {
    if (StringUtils.isBlank(searchKeyword)) {
      return null;
    }

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_SUGGESTER_QUERY_FILE);
    String query = queryTemplate.replace("$SEARCHKEYWORD", searchKeyword.toLowerCase());

    if (StringUtils.isNotBlank(field)) {
      query = query.replace("$FIELD", field.toLowerCase());
    }

    ObjectNode suggestNode = _OM.createObjectNode();
    try {
      ObjectNode textNode = (ObjectNode) _OM.readTree(query);
      suggestNode.putPOJO("suggest", textNode);
    } catch (Exception e) {
      log.error("suggest Exception = " + e.getMessage());
    }
    log.info("suggestNode is " + suggestNode.toString());
    return suggestNode;
  }

  public static ObjectNode generateElasticSearchQueryString(String category, String source, String keywords)
      throws IOException {
    if (StringUtils.isBlank(keywords)) {
      return null;
    }

    List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_DATASET_QUERY_FILE);

    String[] values = keywords.trim().split(",");
    if (StringUtils.isNotBlank(category)) {
      if (category.equalsIgnoreCase(METRIC_CATEGORY)) {
        queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_METRIC_QUERY_FILE);
        ;
      } else if (category.equalsIgnoreCase(COMMENT_CATEGORY)) {
        queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_COMMENT_QUERY_FILE);
      } else if (category.equalsIgnoreCase(FLOW_CATEGORY) || category.equalsIgnoreCase(JOB_CATEGORY)) {
        queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_FLOW_QUERY_FILE);
      }
    }

    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        String query = queryTemplate.replace("$VALUE", value.replace("\"", "").toLowerCase().trim());
        shouldValueList.add(_OM.readTree(query));
      }
    }

    ObjectNode shouldNode = _OM.createObjectNode();
    shouldNode.putPOJO("should", shouldValueList);
    ObjectNode queryNode = _OM.createObjectNode();
    queryNode.putPOJO("bool", shouldNode);

    return queryNode;
  }

  public static ObjectNode generateESFilterStringForSources(String sources) throws IOException {
    if (StringUtils.isBlank(sources)) {
      return null;
    }

    List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_FILTER_UNIT_FILE);
    String[] values = sources.trim().split(",");

    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        String query = queryTemplate.replace("$SOURCE", value.replace("\"", "").toLowerCase().trim());
        shouldValueList.add(_OM.readTree(query));
      }
    }

    ObjectNode shouldNode = _OM.createObjectNode();
    shouldNode.putPOJO("should", shouldValueList);
    ObjectNode queryNode = _OM.createObjectNode();
    queryNode.putPOJO("bool", shouldNode);

    return queryNode;
  }

  public static ObjectNode generateESFilterStringForFabrics(String fabrics) throws IOException {

    if (StringUtils.isBlank(fabrics)) {
      return null;
    }

    List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_FILTER_UNIT_FILE_FABRIC);
    String[] values = fabrics.trim().split(",");

    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        String query = queryTemplate.replace("$FABRIC", value.replace("\"", "").toLowerCase().trim());
        shouldValueList.add(_OM.readTree(query));
      }
    }

    ObjectNode shouldNode = _OM.createObjectNode();
    shouldNode.putPOJO("should", shouldValueList);
    ObjectNode queryNode = _OM.createObjectNode();
    queryNode.putPOJO("bool", shouldNode);

    return queryNode;
  }

  public static ObjectNode generateElasticSearchFilterString(String sources, String fabrics) throws Exception {

    ObjectNode sourcesNode = generateESFilterStringForSources(sources);
    ObjectNode fabricsNode = generateESFilterStringForFabrics(fabrics);

    if (sourcesNode == null && fabricsNode == null) {
      return null;
    }

    List<JsonNode> mustValueList = new ArrayList<JsonNode>();

    if (sourcesNode != null) {
      mustValueList.add(generateESFilterStringForSources(sources));
    }

    if (fabricsNode != null) {
      mustValueList.add(generateESFilterStringForFabrics(fabrics));
    }

    ObjectNode mustNode = _OM.createObjectNode();
    mustNode.putPOJO("must", mustValueList);
    ObjectNode queryNode = _OM.createObjectNode();
    queryNode.putPOJO("bool", mustNode);

    return queryNode;
  }
}