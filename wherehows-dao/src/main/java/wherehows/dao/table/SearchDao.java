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
package wherehows.dao.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import wherehows.models.table.Dataset;
import wherehows.util.HttpUtil;

import static wherehows.util.Search.*;


@Slf4j
public class SearchDao {

  private static final ObjectMapper _OM = new ObjectMapper();

  public List<String> getAutoCompleteList(String elasticSearchUrl, String input, String facet, int limit) {
    return getAutoCompleteListDataset(elasticSearchUrl, input, facet, limit / 3);
  }

  public List<String> getAutoCompleteListDataset(String elasticSearchUrl, String input, String facet, int limit) {
    return getAutoCompleteListbyES(elasticSearchUrl, input, facet, limit);
  }

  public List<String> getAutoCompleteListbyES(String elasticSearchUrl, String input, String fieldName, int limit) {
    // use elastic search completion suggester, ES will validate the input and limit
    List<String> completionSuggestionList = new ArrayList<>();
    Set<String> completionSuggestionSet = new HashSet<>();

    fieldName = "name_suggest";
    ObjectNode keywordNode = generateElasticSearchCompletionSuggesterQuery(fieldName, input, limit);
    log.info("The completion suggester query sent to Elastic Search was: " + keywordNode);

    JsonNode responseNode = null;
    try {
      responseNode = HttpUtil.httpPostRequest(elasticSearchUrl, keywordNode);
    } catch (IOException ex) {
      log.error("ES suggetion list query error: {}" + ex.toString());
    }
    if (responseNode == null || !responseNode.isContainerNode()) {
      return completionSuggestionList;
    }

    JsonNode suggestNode = responseNode.get("suggest");
    if (suggestNode == null || !suggestNode.has("wh-suggest")) {
      log.error("Elastic search completion suggester response does not contain suggest node");
      return completionSuggestionList;
    }

    JsonNode whSuggestNode = suggestNode.get("wh-suggest");
    if (whSuggestNode == null || !whSuggestNode.isArray()) {
      log.error("Elastic search completion suggester response does not contain wh-suggest node");
      return completionSuggestionList;
    }

    Iterator<JsonNode> arrayIterator = whSuggestNode.elements();
    if (arrayIterator == null) {
      return completionSuggestionList;
    }

    while (arrayIterator.hasNext()) {
      JsonNode node = arrayIterator.next();
      if (!node.isContainerNode() || !node.has("options")) {
        continue;
      }

      JsonNode optionsNode = node.get("options");
      if (optionsNode == null || !optionsNode.isArray()) {
        continue;
      }

      Iterator<JsonNode> arrayIteratorOptions = optionsNode.elements();
      if (arrayIteratorOptions == null) {
        continue;
      }

      while (arrayIteratorOptions.hasNext()) {
        JsonNode textNode = arrayIteratorOptions.next();
        if (textNode == null || !textNode.has("text")) {
          continue;
        }
        String oneSuggestion = textNode.get("text").asText();
        completionSuggestionSet.add(oneSuggestion);
      }
    }

    completionSuggestionList.addAll(completionSuggestionSet);
    log.info("Returned suggestion list is: " + completionSuggestionList);
    return completionSuggestionList;
  }

  // this is for did you mean feature
  public List<String> getSuggestionList(String elasticSearchUrl, String category, String searchKeyword) {
    List<String> suggestionList = new ArrayList<>();
    String fieldName = "name";

    ObjectNode keywordNode = generateElasticSearchPhraseSuggesterQuery(fieldName, searchKeyword);

    log.info("The suggest query sent to Elastic Search is: " + keywordNode);

    JsonNode responseNode = null;
    try {
      responseNode = HttpUtil.httpPostRequest(elasticSearchUrl, keywordNode);
    } catch (IOException ex) {
      log.error("ES suggetion list query error: {}" + ex.toString());
    }

    if (responseNode == null || !responseNode.isContainerNode() || !responseNode.has("hits")) {
      return suggestionList;
    }

    JsonNode suggestNode = responseNode.get("suggest");
    log.info("suggestNode is " + suggestNode);

    if (suggestNode == null || !suggestNode.has("simple_phrase")) {
      return suggestionList;
    }

    JsonNode simplePhraseNode = suggestNode.get("simple_phrase");
    if (simplePhraseNode == null || !simplePhraseNode.isArray()) {
      return suggestionList;
    }

    Iterator<JsonNode> arrayIterator = simplePhraseNode.elements();
    if (arrayIterator == null) {
      return suggestionList;
    }

    while (arrayIterator.hasNext()) {
      JsonNode node = arrayIterator.next();
      if (!node.isContainerNode() || !node.has("options")) {
        continue;
      }

      JsonNode optionsNode = node.get("options");
      if (optionsNode == null || !optionsNode.isArray()) {
        continue;
      }

      Iterator<JsonNode> arrayIteratorOptions = optionsNode.elements();
      if (arrayIteratorOptions == null) {
        continue;
      }

      while (arrayIteratorOptions.hasNext()) {
        JsonNode textNode = arrayIteratorOptions.next();
        if (textNode == null || !textNode.has("text")) {
          continue;
        }
        String oneSuggestion = textNode.get("text").asText();
        suggestionList.add(oneSuggestion);
      }
    }

    return suggestionList;
  }

  public JsonNode elasticSearchDatasetByKeyword(String elasticSearchUrl, String category, String keywords,
      String source, int page, int size, String fabric) {
    ObjectNode queryNode = _OM.createObjectNode();
    queryNode.put("from", (page - 1) * size);
    queryNode.put("size", size);
    JsonNode responseNode = null;

    try {
      ObjectNode keywordNode = generateElasticSearchQueryString(category, source, keywords);

      ObjectNode fieldValueFactorNode = _OM.createObjectNode();
      fieldValueFactorNode.put("field", "static_boosting_score");
      fieldValueFactorNode.put("factor", 1);
      fieldValueFactorNode.put("modifier", "square");
      fieldValueFactorNode.put("missing", 1);

      ObjectNode funcScoreNodes = _OM.createObjectNode();
      funcScoreNodes.put("query", keywordNode);
      funcScoreNodes.put("field_value_factor", fieldValueFactorNode);

      ObjectNode funcScoreNodesWrapper = _OM.createObjectNode();
      funcScoreNodesWrapper.put("function_score", funcScoreNodes);

      queryNode.put("query", funcScoreNodesWrapper);

      ObjectNode filterNode = _OM.createObjectNode();
      try {
        filterNode = generateElasticSearchFilterString(source, fabric);
      } catch (Exception e) {
        log.error("Elastic search filter query node generation failed :" + e.getMessage());
      }

      if (filterNode != null) {
        queryNode.put("post_filter", filterNode);
      }

      log.info(" === elasticSearchDatasetByKeyword === The query sent to Elastic Search is: " + queryNode.toString());

      responseNode = HttpUtil.httpPostRequest(elasticSearchUrl, queryNode);
    } catch (IOException e) {
      log.error("Elastic search dataset query error: {}", e.toString());
    }

    ObjectNode resultNode = _OM.createObjectNode();
    long count = 0L;
    List<Dataset> pagedDatasets = new ArrayList<>();
    resultNode.put("page", page);
    resultNode.put("category", category);
    resultNode.put("source", source);
    resultNode.put("itemsPerPage", size);
    resultNode.put("keywords", keywords);

    if (responseNode != null && responseNode.isContainerNode() && responseNode.has("hits")) {
      JsonNode hitsNode = responseNode.get("hits");
      if (hitsNode != null) {
        if (hitsNode.has("total")) {
          count = hitsNode.get("total").asLong();
        }
        if (hitsNode.has("hits")) {
          JsonNode dataNode = hitsNode.get("hits");
          if (dataNode != null && dataNode.isArray()) {
            Iterator<JsonNode> arrayIterator = dataNode.elements();
            if (arrayIterator != null) {
              while (arrayIterator.hasNext()) {
                JsonNode node = arrayIterator.next();
                if (node.isContainerNode() && node.has("_id")) {
                  Dataset dataset = new Dataset();
                  dataset.id = node.get("_id").asLong();
                  if (node.has("_source")) {
                    JsonNode sourceNode = node.get("_source");
                    if (sourceNode != null) {
                      if (sourceNode.has("name")) {
                        dataset.name = sourceNode.get("name").asText();
                      }
                      if (sourceNode.has("source")) {
                        dataset.source = sourceNode.get("source").asText();
                      }
                      if (sourceNode.has("urn")) {
                        dataset.urn = sourceNode.get("urn").asText();
                      }
                      if (sourceNode.has("schema")) {
                        dataset.schema = sourceNode.get("schema").asText();
                      }
                    }
                  }
                  pagedDatasets.add(dataset);
                }
              }
            }
          }
        }
      }
    }
    resultNode.put("count", count);
    resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
    resultNode.set("data", _OM.valueToTree(pagedDatasets));
    return resultNode;
  }
}
