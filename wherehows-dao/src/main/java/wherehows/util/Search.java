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
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Paths;
import java.nio.file.Files;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class Search {
  public final static String DATASET_CATEGORY = "datasets";
  public final static String METRIC_CATEGORY = "metrics";
  public final static String COMMENT_CATEGORY = "comments";
  public final static String FLOW_CATEGORY = "flows";
  public final static String JOB_CATEGORY = "jobs";

  private static final String WHZ_ELASTICSEARCH_DATASET_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_DATASET_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_METRIC_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_METRIC_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_FLOW_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_FLOW_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_COMMENT_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_COMMENT_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_SUGGESTER_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_SUGGESTER_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_AUTO_COMPLETION_QUERY_FILE = System.getenv("WHZ_ELASTICSEARCH_AUTO_COMPLETION_QUERY_TEMPLATE");
  private static final String WHZ_ELASTICSEARCH_FILTER_UNIT_FILE = System.getenv("WHZ_ELASTICSEARCH_FILTER_UNIT");

  public static String readJsonQueryFile(String jsonFile) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();

      String contents = new String(Files.readAllBytes(Paths.get(jsonFile)));
      JsonNode json = objectMapper.readTree(contents);
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

    ObjectNode suggestNode = new ObjectMapper().createObjectNode();

    try {
      ObjectNode textNode = (ObjectNode) new ObjectMapper().readTree(query);
      suggestNode.putPOJO("suggest", textNode);
    } catch (Exception e) {
      log.error("suggest Exception = " + e.getMessage());
    }

    log.info("completionSuggesterQuery suggestNode is " + suggestNode.toString());
    return suggestNode;
  }

  public static ObjectNode generateElasticSearchPhraseSuggesterQuery(String category, String field,
      String searchKeyword) {
    if (StringUtils.isBlank(searchKeyword)) {
      return null;
    }

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_SUGGESTER_QUERY_FILE);
    String query = queryTemplate.replace("$SEARCHKEYWORD", searchKeyword.toLowerCase());

    if (StringUtils.isNotBlank(field)) {
      query = query.replace("$FIELD", field.toLowerCase());
    }

    ObjectNode suggestNode = new ObjectMapper().createObjectNode();
    try {
      ObjectNode textNode = (ObjectNode) new ObjectMapper().readTree(query);
      suggestNode.putPOJO("suggest", textNode);
    } catch (Exception e) {
      log.error("suggest Exception = " + e.getMessage());
    }
    log.info("suggestNode is " + suggestNode.toString());
    return suggestNode;
  }

  public static ObjectNode generateElasticSearchQueryString(String category, String source, String keywords) throws Exception
  {
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

    ObjectMapper objectMapper = new ObjectMapper();

    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        String query = queryTemplate.replace("$VALUE", value.replace("\"", "").toLowerCase().trim());
        shouldValueList.add(objectMapper.readTree(query));
      }
    }

    ObjectNode shouldNode = new ObjectMapper().createObjectNode();
    shouldNode.putPOJO("should", shouldValueList);
    ObjectNode queryNode = new ObjectMapper().createObjectNode();
    queryNode.putPOJO("bool", shouldNode);

    return queryNode;
  }


  public static ObjectNode generateElasticSearchFilterString(String sources) throws Exception{
    if (StringUtils.isBlank(sources)) {
      return null;
    }

    List<JsonNode> shouldValueList = new ArrayList<JsonNode>();

    String queryTemplate = readJsonQueryFile(WHZ_ELASTICSEARCH_FILTER_UNIT_FILE);
    String[] values = sources.trim().split(",");

    ObjectMapper objectMapper = new ObjectMapper();

    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        String query = queryTemplate.replace("$SOURCE", value.replace("\"", "").toLowerCase().trim());
        shouldValueList.add(objectMapper.readTree(query));
      }
    }

    ObjectNode shouldNode = new ObjectMapper().createObjectNode();
    shouldNode.putPOJO("should", shouldValueList);
    ObjectNode queryNode = new ObjectMapper().createObjectNode();
    queryNode.putPOJO("bool", shouldNode);

    return queryNode;
  }
}