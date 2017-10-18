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
package dao;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.Play;
import play.libs.F.Promise;
import play.libs.Json;
import play.libs.ws.*;
import wherehows.models.table.Dataset;
import wherehows.models.table.FlowJob;
import wherehows.models.table.Metric;


public class SearchDAO extends AbstractMySQLOpenSourceDAO
{
	public static String ELASTICSEARCH_DATASET_URL_KEY = "elasticsearch.dataset.url";

	public static String ELASTICSEARCH_METRIC_URL_KEY = "elasticsearch.metric.url";

	public static String ELASTICSEARCH_FLOW_URL_KEY = "elasticsearch.flow.url";

	public static String WHEREHOWS_SEARCH_ENGINE_KEY = "search.engine";

	private final static String SEARCH_DATASET_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, `name`, `schema`, `source`, `urn`, FROM_UNIXTIME(source_modified_time) as modified, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 as rank " +
			"FROM (SELECT id, `name`, `schema`, `source`, `urn`, source_modified_time, " +
			"CASE WHEN `name` = '$keyword' THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN `name` like '$keyword%' THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN `name` like '%$keyword%' THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN `urn` = '$keyword' THEN 300 ELSE 0 END rank_04, " +
			"CASE WHEN `urn` like '$keyword%' THEN 200 ELSE 0 END rank_05, " +
			"CASE WHEN `urn` like '%$keyword%' THEN 100 ELSE 0 END rank_06, " +
			"CASE WHEN `schema` = '$keyword' THEN 30 ELSE 0 END rank_07, " +
			"CASE WHEN `schema` like '$keyword%' THEN 20 ELSE 0 END rank_08, " +
			"CASE WHEN `schema` like '%$keyword%' THEN 10 ELSE 0 END rank_09 " +
			"FROM dict_dataset WHERE MATCH(`name`, `schema`,  `properties`, `urn`)" +
			" AGAINST ('*$keyword* *v_$keyword*' IN BOOLEAN MODE) ) t " +
			"ORDER BY rank DESC, `name`, `urn` LIMIT ?, ?;";

	private final static String SEARCH_DATASET_BY_SOURCE_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, `name`, `schema`, `source`, `urn`, FROM_UNIXTIME(source_modified_time) as modified, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 as rank " +
			"FROM (SELECT id, `name`, `schema`, `source`, `urn`, source_modified_time, " +
			"CASE WHEN `name` = '$keyword' THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN `name` like '$keyword%' THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN `name` like '%$keyword%' THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN `urn` = '$keyword' THEN 300 ELSE 0 END rank_04, " +
			"CASE WHEN `urn` like '$keyword%' THEN 200 ELSE 0 END rank_05, " +
			"CASE WHEN `urn` like '%$keyword%' THEN 100 ELSE 0 END rank_06, " +
			"CASE WHEN `schema` = '$keyword' THEN 30 ELSE 0 END rank_07, " +
			"CASE WHEN `schema` like '$keyword%' THEN 20 ELSE 0 END rank_08, " +
			"CASE WHEN `schema` like '%$keyword%' THEN 10 ELSE 0 END rank_09 " +
			"FROM dict_dataset WHERE MATCH(`name`, `schema`,  `properties`, `urn`)" +
			" AGAINST ('*$keyword* *v_$keyword*' IN BOOLEAN MODE) and source = ? ) t " +
			"ORDER BY rank desc, `name`, `urn` LIMIT ?, ?;";

	private final static String SEARCH_FLOW_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"a.app_code, f.app_id, f.flow_id, f.flow_name, f.flow_group, f.flow_path, f.flow_level, " +
			"rank_01 + rank_02 + rank_03 + rank_04 as rank " +
			"FROM (SELECT app_id, flow_id, flow_name, flow_group, flow_path, flow_level, " +
			"CASE WHEN flow_name = '$keyword' THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN flow_name like '%$keyword' THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN flow_name like '$keyword%' THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN flow_name like '%$keyword%' THEN 100 ELSE 0 END rank_04 " +
			"FROM flow WHERE flow_name like '%$keyword%' ) f " +
			"JOIN cfg_application a on f.app_id = a.app_id ORDER BY " +
			"rank DESC, flow_name, app_id, flow_id, flow_group, flow_path LIMIT ?, ?";

	private final static String SEARCH_JOB_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"a.app_code, f.flow_name, f.flow_group, f.flow_path, f.flow_level, " +
			"j.app_id, j.flow_id, j.job_id, j.job_name, j.job_path, j.job_type, " +
			"rank_01+rank_02+rank_03+rank_04 as rank " +
			"FROM (SELECT app_id, flow_id, job_id, job_name, job_path, job_type, " +
			"CASE WHEN job_name = '$keyword' THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN job_name like '%$keyword' THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN job_name like '$keyword%' THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN job_name like '%$keyword%' THEN 100 ELSE 0 END rank_04 " +
			"FROM flow_job WHERE job_name like '%$keyword%' ) j " +
			"JOIN cfg_application a on a.app_id = j.app_id " +
			"JOIN flow f on f.app_id = j.app_id AND f.flow_id = j.flow_id " +
			"ORDER BY rank DESC, j.job_name, j.app_id, j.flow_id, j.job_id, j.job_path LIMIT ?, ?";

	private final static String SEARCH_METRIC_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"metric_id, `metric_name`, `metric_description`, `dashboard_name`, `metric_group`, `metric_category`, " +
			"`metric_sub_category`, `metric_level`, `metric_source_type`, `metric_source`, `metric_source_dataset_id`, " +
			"`metric_ref_id_type`, `metric_ref_id`, `metric_type`, `metric_grain`, `metric_display_factor`, " +
			"`metric_display_factor_sym`, `metric_good_direction`, `metric_formula`, `dimensions`, " +
			"`owners`, `tags`, `urn`, `metric_url`, `wiki_url`, `scm_url`, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 + rank_10 + " +
			"rank_11 + rank_12 + rank_13 + rank_14 + rank_15 + rank_16 + rank_17 + rank_18 + rank_19 + rank_20 as rank, " +
			"0 as watch_id " +
			"FROM (SELECT metric_id, `metric_name`, `metric_description`, `dashboard_name`, `metric_group`, " +
			"`metric_category`, `metric_sub_category`, `metric_level`, `metric_source_type`, " +
			"`metric_source`, `metric_source_dataset_id`, `metric_ref_id_type`, `metric_ref_id`, `metric_type`, " +
			"`metric_grain`, `metric_display_factor`, `metric_display_factor_sym`, `metric_good_direction`, " +
			"`metric_formula`, `dimensions`, `owners`, `tags`, `urn`, `metric_url`, `wiki_url`, `scm_url`, " +
			"CASE WHEN `metric_name` = '$keyword' THEN 90000 ELSE 0 END rank_01, " +
			"CASE WHEN `metric_name` like '%$keyword' THEN 30000 ELSE 0 END rank_02, " +
			"CASE WHEN `metric_name` like '$keyword%'THEN 20000 ELSE 0 END rank_03, " +
			"CASE WHEN `metric_name` like '%$keyword%'THEN 10000 ELSE 0 END rank_04, " +
			"CASE WHEN `metric_description` = '$keyword' THEN 9000 ELSE 0 END rank_05, " +
			"CASE WHEN `metric_description` like '%$keyword' THEN 3000 ELSE 0 END rank_06, " +
			"CASE WHEN `metric_description` like '$keyword%' THEN 2000 ELSE 0 END rank_07, " +
			"CASE WHEN `metric_description` like '%$keyword%' THEN 1000 ELSE 0 END rank_08, " +
			"CASE WHEN `metric_category` = '$keyword' THEN 900 ELSE 0 END rank_09, " +
			"CASE WHEN `metric_category` like '%$keyword' THEN 300 ELSE 0 END rank_10, " +
			"CASE WHEN `metric_category` like '$keyword%' THEN 200 ELSE 0 END rank_11, " +
			"CASE WHEN `metric_category` like '%$keyword%' THEN 100 ELSE 0 END rank_12, " +
			"CASE WHEN `metric_group` = '$keyword' THEN 90 ELSE 0 END rank_13, " +
			"CASE WHEN `metric_group` like '%$keyword' THEN 30 ELSE 0 END rank_14, " +
			"CASE WHEN `metric_group` like '$keyword%' THEN 20 ELSE 0 END rank_15, " +
			"CASE WHEN `metric_group` like '%$keyword%' THEN 10 ELSE 0 END rank_16, " +
			"CASE WHEN `dashboard_name` = '$keyword' THEN 9 ELSE 0 END rank_17, " +
			"CASE WHEN `dashboard_name` like '%$keyword' THEN 3 ELSE 0 END rank_18, " +
			"CASE WHEN `dashboard_name` like '$keyword%' THEN 2 ELSE 0 END rank_19, " +
			"CASE WHEN `dashboard_name` like '%$keyword%' THEN 1 ELSE 0 END rank_20 " +
			"FROM dict_business_metric WHERE " +
			"`metric_name` like '%$keyword%' or `dashboard_name` like '%$keyword%' or `metric_group` like '%$keyword%' " +
			"or `metric_category` like '%$keyword%' or `metric_description` like '%$keyword%' ) m ORDER BY " +
			"rank DESC, `metric_name`, `metric_category`, `metric_group`, `dashboard_name` LIMIT ?, ?;";

	private final static String SEARCH_DATASET_BY_COMMENTS_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, source, urn, `schema` FROM dict_dataset WHERE id in " +
			"(SELECT dataset_id FROM comments WHERE MATCH(text) against ('*$keyword*' in BOOLEAN MODE) ) " +
			"UNION ALL SELECT id, name, source, urn, `schema` FROM dict_dataset WHERE id in " +
			"(SELECT fd.dataset_id FROM ( " +
			"SELECT id FROM field_comments fc WHERE " +
			"MATCH(comment) AGAINST ('*$keyword*' IN BOOLEAN MODE) ) c JOIN dict_field_detail fd " +
			"ON ( find_in_set(c.id, fd.comment_ids) or c.id = fd.default_comment_id )) " +
			"ORDER BY 2 LIMIT ?, ?;";

	public static List<String> getAutoCompleteList(String input, int limit)
	{
		List<String> names = new ArrayList<>();
		names.addAll(getAutoCompleteListDataset(input, limit / 3));
		names.addAll(getAutoCompleteListMetric(input, limit / 3));
		names.addAll(getAutoCompleteListFlow(input, limit / 3));
		return names;
	}

	public static List<String> getAutoCompleteListDataset(String input, int limit)
	{
		String elasticSearchTypeURLKey = "elasticsearch.dataset.url";
		String fieldName = "name_suggest";
		return getAutoCompleteListbyES(elasticSearchTypeURLKey,fieldName,input,limit);
	}

	public static List<String> getAutoCompleteListMetric(String input, int limit)
	{
		String elasticSearchTypeURLKey = "elasticsearch.metric.url";
		String fieldName = "metric_name_suggest";
		return getAutoCompleteListbyES(elasticSearchTypeURLKey,fieldName,input,limit);
	}

	public static List<String> getAutoCompleteListFlow(String input, int limit)
	{
		String elasticSearchTypeURLKey = "elasticsearch.flow.url";
		String fieldName = "flow_name_suggest";
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
    	keywordNode = utils.Search.generateElasticSearchCompletionSuggesterQuery(fieldName, input, limit);
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

      switch (lCategory) {
        case "dataset":
          elasticSearchType = "dataset";
          elasticSearchTypeURLKey = "elasticsearch.dataset.url";
          fieldName = "name";
          break;
        case "metric":
          elasticSearchType = "metric";
          elasticSearchTypeURLKey = "elasticsearch.metric.url";
          fieldName = "metric_name";
          break;
        case "flow":
          elasticSearchType = "flow";
          elasticSearchTypeURLKey = "elasticsearch.flow.url";
          fieldName = "flow_name";
          break;
        default:
          break;
      }

      keywordNode = utils.Search.generateElasticSearchPhraseSuggesterQuery(elasticSearchType, fieldName, searchKeyword);
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
			keywordNode = utils.Search.generateElasticSearchQueryString(category, source, keywords);
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
				filterNode = utils.Search.generateElasticSearchFilterString(source);
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

	public static JsonNode elasticSearchMetricByKeyword(
			String category,
			String keywords,
			int page,
			int size)
	{
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);
		JsonNode responseNode = null;
		ObjectNode keywordNode = null;

		try
		{
			keywordNode = utils.Search.generateElasticSearchQueryString(category, null, keywords);
		}
		catch(Exception e)
		{
			Logger.error("Elastic search metric input query is not JSON format. Error message :" + e.getMessage());
		}

		if (keywordNode != null)
		{
			queryNode.set("query", keywordNode);

			Logger.info(" === elasticSearchMetricByKeyword === The query sent to Elastic Search is: " + queryNode.toString());

			Promise<WSResponse> responsePromise = WS.url(Play.application().configuration().getString(
					SearchDAO.ELASTICSEARCH_METRIC_URL_KEY)).post(queryNode);
			responseNode = responsePromise.get(1000).asJson();
		}

		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<Metric> pagedMetrics = new ArrayList<>();
		resultNode.put("page", page);
		resultNode.put("category", category);
		resultNode.put("isMetrics", true);
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
									Metric metric = new Metric();
									metric.id = node.get("_id").asInt();
									if (node.has("_source")) {
										JsonNode sourceNode = node.get("_source");
										if (sourceNode != null) {
											if (sourceNode.has("metric_name")) {
												metric.name = sourceNode.get("metric_name").asText();
											}
											if (sourceNode.has("metric_description")) {
												metric.description = sourceNode.get("metric_description").asText();
											}
											if (sourceNode.has("dashboard_name")) {
												metric.dashboardName = sourceNode.get("dashboard_name").asText();
											}
											if (sourceNode.has("metric_group")) {
												metric.group = sourceNode.get("metric_group").asText();
											}
											if (sourceNode.has("metric_category")) {
												metric.category = sourceNode.get("metric_category").asText();
											}
											if (sourceNode.has("urn")) {
												metric.urn = sourceNode.get("urn").asText();
											}
											if (sourceNode.has("metric_source")) {
												metric.source = sourceNode.get("metric_source").asText();
												if (StringUtils.isBlank(metric.source))
												{
													metric.source = null;
												}
											}
											metric.schema = sourceNode.toString();
										}
									}
									pagedMetrics.add(metric);
								}
							}
						}

					}
				}

			}
		}
		resultNode.put("count", count);
		resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
		resultNode.set("data", Json.toJson(pagedMetrics));
		return resultNode;
	}

	public static JsonNode elasticSearchFlowByKeyword(
			String category,
			String keywords,
			int page,
			int size)
	{
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);
		JsonNode searchOpt = null;
		JsonNode responseNode = null;
		ObjectNode keywordNode = null;

		try
		{
			keywordNode = utils.Search.generateElasticSearchQueryString(category, null, keywords);
		}
		catch(Exception e)
		{
			Logger.error("Elastic search flow input query is not JSON format. Error message :" + e.getMessage());
		}

		if (keywordNode != null)
		{
			queryNode.set("query", keywordNode);

			Logger.info(" === elasticSearchFlowByKeyword === The query sent to Elastic Search is: " + queryNode.toString());

			Promise<WSResponse> responsePromise = WS.url(Play.application().configuration().getString(
					SearchDAO.ELASTICSEARCH_FLOW_URL_KEY)).post(queryNode);
			responseNode = responsePromise.get(1000).asJson();
		}

		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<FlowJob> pagedFlowJobs = new ArrayList<>();
		resultNode.put("page", page);
		resultNode.put("category", category);
		resultNode.put("isFlowJob", true);
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
									FlowJob flowJob = new FlowJob();
									if (node.has("_source")) {
										JsonNode sourceNode = node.get("_source");
										if (sourceNode != null) {
											if (sourceNode.has("app_code")) {
												flowJob.appCode = sourceNode.get("app_code").asText();
											}
											if (sourceNode.has("app_id")) {
												flowJob.appId = sourceNode.get("app_id").asInt();
											}
											if (sourceNode.has("flow_id")) {
												flowJob.flowId = sourceNode.get("flow_id").asLong();
											}
											if (sourceNode.has("flow_name")) {
												flowJob.flowName = sourceNode.get("flow_name").asText();
												flowJob.displayName = flowJob.flowName;
											}
											if (sourceNode.has("flow_path")) {
												flowJob.flowPath = sourceNode.get("flow_path").asText();
											}
											if (sourceNode.has("flow_group")) {
												flowJob.flowGroup = sourceNode.get("flow_group").asText();
											}
											flowJob.link = "#/flows/name/" + flowJob.appCode + "/" +
													Long.toString(flowJob.flowId) + "/page/1?urn=" + flowJob.flowGroup;
											flowJob.path = flowJob.appCode + "/" + flowJob.flowPath;

											flowJob.schema = sourceNode.toString();
										}
									}
									pagedFlowJobs.add(flowJob);
								}
							}
						}

					}
				}

			}
		}
		resultNode.put("count", count);
		resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
		resultNode.set("data", Json.toJson(pagedFlowJobs));
		return resultNode;
	}

	public static ObjectNode getPagedDatasetByKeyword(String category, String keyword, String source, int page, int size)
	{
    	List<Dataset> pagedDatasets = new ArrayList<Dataset>();
		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				List<Map<String, Object>> rows = null;
				try{
					if (StringUtils.isBlank(source) || source.toLowerCase().equalsIgnoreCase("all"))
					{
						String query = SEARCH_DATASET_WITH_PAGINATION.replace("$keyword", keyword);
						rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
					}
					else
					{
						String query = SEARCH_DATASET_BY_SOURCE_WITH_PAGINATION.replace("$keyword", keyword);
						rows = jdbcTemplate.queryForList(query, source, (page-1)*size, size);
					}
				}
				catch (Exception e)
				{
					Logger.error(e.getMessage());
				}

				if (rows != null)
				{
					for (Map row : rows) {

						Dataset ds = new Dataset();
						ds.id = (Long)row.get(DatasetRowMapper.DATASET_ID_COLUMN);
						ds.name = (String)row.get(DatasetRowMapper.DATASET_NAME_COLUMN);
						ds.source = (String)row.get(DatasetRowMapper.DATASET_SOURCE_COLUMN);
						ds.urn = (String)row.get(DatasetRowMapper.DATASET_URN_COLUMN);
						ds.schema = (String)row.get(DatasetRowMapper.DATASET_SCHEMA_COLUMN);
						pagedDatasets.add(ds);
					}
				}

				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("category", category);
				resultNode.put("source", source);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedDatasets));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedMetricByKeyword(final String category, String keyword, int page, int size)
	{
		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				String query = SEARCH_METRIC_WITH_PAGINATION.replace("$keyword", keyword);
				List<Metric> pagedMetrics = new ArrayList<Metric>();
				try				{

					pagedMetrics = jdbcTemplate.query(
							query,
							new MetricRowMapper(),
							(page - 1) * size, size);
				}
				catch(Exception e)
				{
					Logger.error(e.getMessage());
				}

				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("isMetrics", true);
				resultNode.put("category", category);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedMetrics));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedFlowByKeyword(String category, String keyword, int page, int size)
	{
		final List<FlowJob> pagedFlows = new ArrayList<FlowJob>();
		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				String query = SEARCH_FLOW_WITH_PAGINATION.replace("$keyword", keyword);
				List<Map<String, Object>> rows = null;
				try
				{
					rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
				}
				catch (Exception e)
				{
					Logger.error(e.getMessage());
				}

				if (rows != null)
				{
					for (Map row : rows) {

						FlowJob flow = new FlowJob();
						flow.flowId = (Long)row.get(FlowRowMapper.FLOW_ID_COLUMN);
						flow.flowName = (String)row.get(FlowRowMapper.FLOW_NAME_COLUMN);
						flow.flowPath = (String)row.get(FlowRowMapper.FLOW_PATH_COLUMN);
						flow.flowGroup = (String)row.get(FlowRowMapper.FLOW_GROUP_COLUMN);
						flow.appCode = (String)row.get(FlowRowMapper.APP_CODE_COLUMN);
						flow.appId = (Integer)row.get(FlowRowMapper.APP_ID_COLUMN);
						flow.displayName = flow.flowName;
						flow.link = "#/flows/" + flow.appCode + "/" +
								flow.flowGroup + "/" + Long.toString(flow.flowId) + "/page/1";
						flow.path = flow.appCode + "/" + flow.flowPath;
						pagedFlows.add(flow);
					}
				}

				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("isFlowJob", true);
				resultNode.put("page", page);
				resultNode.put("category", category);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedFlows));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedJobByKeyword(String category, String keyword, int page, int size)
	{
		final List<FlowJob> pagedFlowJobs = new ArrayList<FlowJob>();
		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				String query = SEARCH_JOB_WITH_PAGINATION.replace("$keyword", keyword);
				List<Map<String, Object>> rows = null;

				try
				{
					rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
				}
				catch (Exception e)
				{
					Logger.error(e.getMessage());
				}

				if (rows != null)
				{
					for (Map row : rows) {

						FlowJob flowJob = new FlowJob();
						flowJob.flowId = (Long)row.get(FlowRowMapper.FLOW_ID_COLUMN);
						flowJob.jobId = (Long)row.get(FlowRowMapper.JOB_ID_COLUMN);
						flowJob.jobName = (String)row.get(FlowRowMapper.JOB_NAME_COLUMN);
						flowJob.jobPath = (String)row.get(FlowRowMapper.JOB_PATH_COLUMN);
						flowJob.jobType = (String)row.get(FlowRowMapper.JOB_TYPE_COLUMN);
						flowJob.flowName = (String)row.get(FlowRowMapper.FLOW_NAME_COLUMN);
						flowJob.flowPath = (String)row.get(FlowRowMapper.FLOW_PATH_COLUMN);
						flowJob.flowGroup = (String)row.get(FlowRowMapper.FLOW_GROUP_COLUMN);
						flowJob.appCode = (String)row.get(FlowRowMapper.APP_CODE_COLUMN);
						flowJob.appId = (Integer)row.get(FlowRowMapper.APP_ID_COLUMN);
						flowJob.displayName = flowJob.jobName;
						flowJob.link =  "#/flows/" + flowJob.appCode + "/" +
								flowJob.flowGroup + "/" + Long.toString(flowJob.flowId) + "/page/1";
						flowJob.path = flowJob.appCode + "/" + flowJob.jobPath;

						pagedFlowJobs.add(flowJob);
					}
				}

				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("isFlowJob", true);
				resultNode.put("page", page);
				resultNode.put("category", category);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedFlowJobs));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedCommentsByKeyword(String category, String keyword, int page, int size)
	{
		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				List<Map<String, Object>> rows = null;
				List<Dataset> pagedDatasets = new ArrayList<Dataset>();
				String query = SEARCH_DATASET_BY_COMMENTS_WITH_PAGINATION.replace("$keyword", keyword);
				try{
					rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
				}
				catch (Exception e)
				{
					Logger.error(e.getMessage());
				}

				if (rows != null)
				{
					for (Map row : rows) {

						Dataset ds = new Dataset();
						ds.id = (long)row.get(DatasetRowMapper.DATASET_ID_COLUMN);
						ds.name = (String)row.get(DatasetRowMapper.DATASET_NAME_COLUMN);
						ds.source = (String)row.get(DatasetRowMapper.DATASET_SOURCE_COLUMN);
						ds.urn = (String)row.get(DatasetRowMapper.DATASET_URN_COLUMN);
						ds.schema = (String)row.get(DatasetRowMapper.DATASET_SCHEMA_COLUMN);
						pagedDatasets.add(ds);
					}
				}

				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("category", category);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedDatasets));

				return resultNode;
			}
		});

		return result;
	}

}
