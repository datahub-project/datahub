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
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import play.Logger;
import play.Play;
import play.libs.F.Promise;
import play.libs.Json;
import play.libs.ws.*;
import wherehows.models.table.Dataset;


import static wherehows.util.Search.*;

public class SearchDAO
{
	public static String ELASTICSEARCH_DATASET_URL_KEY = "elasticsearch.dataset.url";
	public static String WHEREHOWS_SEARCH_ENGINE_KEY = "search.engine";

	public static List<String> getAutoCompleteList(String input, int limit)
	{
		List<String> names = new ArrayList<>();
		names.addAll(getAutoCompleteListDataset(input, limit / 3));
		return names;
	}

	public static List<String> getAutoCompleteListDataset(String input, int limit)
	{
		String elasticSearchTypeURLKey = "elasticsearch.dataset.url";
		String fieldName = "name_suggest";
		return getAutoCompleteListbyES(elasticSearchTypeURLKey,fieldName,input,limit);
	}

	public static List<String> getAutoCompleteListbyES(String elasticSearchTypeURLKey, String fieldName, String input,
			int limit)
	{
		// use elastic search completion suggester, ES will validate the input and limit
		List<String> completionSuggestionList = new ArrayList<String>();
		Set<String> completionSuggestionSet = new HashSet<String>();

		JsonNode responseNode = null;
		ObjectNode keywordNode = null;

		try {
			keywordNode = generateElasticSearchCompletionSuggesterQuery(fieldName, input, limit);
		} catch (Exception e) {
			Logger.error("Elastic search completion suggester error. Error message :" + e.getMessage());
		}

		Logger.info("The completion suggester query sent to Elastic Search was: " + keywordNode.toString());

		Promise<WSResponse> responsePromise =
				WS.url(Play.application().configuration().getString(elasticSearchTypeURLKey)).post(keywordNode);
		responseNode = responsePromise.get(1000).asJson();

		if (responseNode == null || !responseNode.isContainerNode()) {
			return completionSuggestionList;
		}

		JsonNode suggestNode = responseNode.get("suggest");
		if (suggestNode == null || !suggestNode.has("wh-suggest")) {
			Logger.error("Elastic search completion suggester response does not contain suggest node");
			return completionSuggestionList;
		}

		JsonNode whSuggestNode = suggestNode.get("wh-suggest");
		if (whSuggestNode == null || !whSuggestNode.isArray()) {
			Logger.error("Elastic search completion suggester response does not contain wh-suggest node");
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
		Logger.info("Returned suggestion list is: " + completionSuggestionList);
		return completionSuggestionList;
	}

	// this is for did you mean feature
	public static List<String> getSuggestionList(String category, String searchKeyword)
	{
		List<String> SuggestionList = new ArrayList<String>();
		String elasticSearchType = "dataset";
		String elasticSearchTypeURLKey = "elasticsearch.dataset.url";
		String fieldName = "name";

		JsonNode responseNode = null;
		ObjectNode keywordNode = null;

		try {
			String lCategory = category.toLowerCase();
			Logger.info("lCategory is " + category);

			// ToDO: deprecate category or reuse for entity
			switch (lCategory) {
				case "dataset":
					elasticSearchType = "dataset";
					elasticSearchTypeURLKey = "elasticsearch.dataset.url";
					fieldName = "name";
					break;
				default:
					break;
			}

			keywordNode = generateElasticSearchPhraseSuggesterQuery(elasticSearchType, fieldName, searchKeyword);
		} catch (Exception e) {
			Logger.error("Elastic search phrase suggester error. Error message :" + e.getMessage());
		}

		Logger.info("The suggest query sent to Elastic Search is: " + keywordNode.toString());

		Promise<WSResponse> responsePromise =
				WS.url(Play.application().configuration().getString(elasticSearchTypeURLKey)).post(keywordNode);
		responseNode = responsePromise.get(1000).asJson();

		if (responseNode == null || !responseNode.isContainerNode() || !responseNode.has("hits")) {
			return SuggestionList;
		}

		JsonNode suggestNode = responseNode.get("suggest");
		Logger.info("suggestNode is " + suggestNode.toString());

		if (suggestNode == null || !suggestNode.has("simple_phrase")) {
			return SuggestionList;
		}

		JsonNode simplePhraseNode = suggestNode.get("simple_phrase");
		if (simplePhraseNode == null || !simplePhraseNode.isArray()) {
			return SuggestionList;
		}

		Iterator<JsonNode> arrayIterator = simplePhraseNode.elements();
		if (arrayIterator == null) {
			return SuggestionList;
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
				SuggestionList.add(oneSuggestion);
			}
		}

		return SuggestionList;
	}

	public static JsonNode elasticSearchDatasetByKeyword(
			String category,
			String keywords,
			String source,
			int page,
			int size)
	{
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);
		JsonNode responseNode = null;
		ObjectNode keywordNode = null;

		try {
			keywordNode = generateElasticSearchQueryString(category, source, keywords);
		} catch (Exception e) {
			Logger.error("Elastic search dataset input query is not JSON format. Error message :" + e.getMessage());
		}

		if (keywordNode != null) {
			ObjectNode funcScoreNodes = Json.newObject();

			ObjectNode fieldValueFactorNode = Json.newObject();
			fieldValueFactorNode.put("field", "static_boosting_score");
			fieldValueFactorNode.put("factor", 1);
			fieldValueFactorNode.put("modifier", "square");
			fieldValueFactorNode.put("missing", 1);

			funcScoreNodes.put("query", keywordNode);
			funcScoreNodes.put("field_value_factor", fieldValueFactorNode);

			ObjectNode funcScoreNodesWrapper = Json.newObject();
			funcScoreNodesWrapper.put("function_score", funcScoreNodes);

			queryNode.put("query", funcScoreNodesWrapper);

			ObjectNode filterNode = Json.newObject();
			try {
				filterNode = generateElasticSearchFilterString(source);
			} catch (Exception e) {
				Logger.error("Elastic search filter query node generation failed :" + e.getMessage());
			}

			if (filterNode != null) {
				queryNode.put("post_filter", filterNode);
			}

			Logger.info(
					" === elasticSearchDatasetByKeyword === The query sent to Elastic Search is: " + queryNode.toString());

			Promise<WSResponse> responsePromise =
					WS.url(Play.application().configuration().getString(SearchDAO.ELASTICSEARCH_DATASET_URL_KEY)).post(queryNode);
			responseNode = responsePromise.get(1000).asJson();

			// Logger.debug("The responseNode from Elastic Search is: " + responseNode.toString());

		}

		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<Dataset> pagedDatasets = new ArrayList<>();
		resultNode.put("page", page);
		resultNode.put("category", category);
		resultNode.put("source", source);
		resultNode.put("itemsPerPage", size);
		resultNode.put("keywords", keywords);

		if (responseNode != null && responseNode.isContainerNode() && responseNode.has("hits"))
		{
			JsonNode hitsNode = responseNode.get("hits");
			if (hitsNode != null)
			{
				if  (hitsNode.has("total"))
				{
					count = hitsNode.get("total").asLong();
				}
				if (hitsNode.has("hits"))
				{
					JsonNode dataNode = hitsNode.get("hits");
					if (dataNode != null && dataNode.isArray())
					{
						Iterator<JsonNode> arrayIterator = dataNode.elements();
						if (arrayIterator != null)
						{
							while (arrayIterator.hasNext())
							{
								JsonNode node = arrayIterator.next();
								if (node.isContainerNode() && node.has("_id"))
								{
									Dataset dataset = new Dataset();
									dataset.id = node.get("_id").asLong();
									if (node.has("_source"))
									{
										JsonNode sourceNode = node.get("_source");
										if (sourceNode != null)
										{
											if (sourceNode.has("name"))
											{
												dataset.name = sourceNode.get("name").asText();
											}
											if (sourceNode.has("source"))
											{
												dataset.source = sourceNode.get("source").asText();
											}
											if (sourceNode.has("urn"))
											{
												dataset.urn = sourceNode.get("urn").asText();
											}
											if (sourceNode.has("schema"))
											{
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
		resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
		resultNode.set("data", Json.toJson(pagedDatasets));
		return resultNode;
	}

}