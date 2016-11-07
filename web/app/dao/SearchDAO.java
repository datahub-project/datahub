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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import play.cache.Cache;
import models.*;

public class SearchDAO extends AbstractMySQLOpenSourceDAO
{
	public static String ELASTICSEARCH_DATASET_URL_KEY = "elasticsearch.dataset.url";

	public static String ELASTICSEARCH_METRIC_URL_KEY = "elasticsearch.metric.url";

	public static String ELASTICSEARCH_FLOW_URL_KEY = "elasticsearch.flow.url";

	public static String WHEREHOWS_SEARCH_ENGINE__KEY = "search.engine";

	public final static String SEARCH_DATASET_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
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

	public final static String SEARCH_DATASET_BY_SOURCE_WITH_PAGINATION = "SELECt SQL_CALC_FOUND_ROWS " +
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

	public final static String SEARCH_FLOW_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
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

	public final static String SEARCH_JOB_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
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

	public final static String SEARCH_METRIC_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"metric_id, `metric_name`, `metric_description`, `dashboard_name`, `metric_ref_id_type`, " +
			"`metric_ref_id`, `metric_category`, `metric_group`, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 + rank_10 + " +
			"rank_11 + rank_12 + rank_13 + rank_14 + rank_15 + rank_16 + rank_17 + rank_18 + rank_19 + rank_20 as rank " +
			"FROM (SELECT metric_id, `metric_name`, `metric_description`, `dashboard_name`, " +
			"`metric_ref_id_type`, `metric_ref_id`, `metric_category`, `metric_group`, " +
			"CASE WHEN match(`metric_name`) against ('$keyword' IN BOOLEAN MODE) THEN 90000 ELSE 0 END rank_01, " +
			"CASE WHEN match(`metric_name`) against ('$keyword' IN BOOLEAN MODE) THEN 30000 ELSE 0 END rank_02, " +
			"CASE WHEN match(`metric_name`) against ('$keyword*' IN BOOLEAN MODE) THEN 20000 ELSE 0 END rank_03, " +
			"CASE WHEN match(`metric_name`) against ('*$keyword*' IN BOOLEAN MODE) THEN 10000 ELSE 0 END rank_04, " +
			"CASE WHEN match(`metric_description`) against ('$keyword' IN BOOLEAN MODE) THEN 9000 ELSE 0 END rank_05, " +
			"CASE WHEN match(`metric_description`) against ('$keyword' IN BOOLEAN MODE) THEN 3000 ELSE 0 END rank_06, " +
			"CASE WHEN match(`metric_description`) against ('$keyword*' IN BOOLEAN MODE) THEN 2000 ELSE 0 END rank_07, " +
			"CASE WHEN match(`metric_description`) against ('*$keyword*' IN BOOLEAN MODE) THEN 1000 ELSE 0 END rank_08, " +
			"CASE WHEN match(`metric_category`) against ('$keyword' IN BOOLEAN MODE) THEN 900 ELSE 0 END rank_09, " +
			"CASE WHEN match(`metric_category`) against ('$keyword' IN BOOLEAN MODE) THEN 300 ELSE 0 END rank_10, " +
			"CASE WHEN match(`metric_category`) against ('$keyword*' IN BOOLEAN MODE) THEN 200 ELSE 0 END rank_11, " +
			"CASE WHEN match(`metric_category`) against ('*$keyword*' IN BOOLEAN MODE) THEN 100 ELSE 0 END rank_12, " +
			"CASE WHEN match(`metric_group`) against ('$keyword' IN BOOLEAN MODE) THEN 90 ELSE 0 END rank_13, " +
			"CASE WHEN match(`metric_group`) against ('$keyword' IN BOOLEAN MODE) THEN 30 ELSE 0 END rank_14, " +
			"CASE WHEN match(`metric_group`) against ('$keyword*' IN BOOLEAN MODE) THEN 20 ELSE 0 END rank_15, " +
			"CASE WHEN match(`metric_group`) against ('*$keyword*' IN BOOLEAN MODE) THEN 10 ELSE 0 END rank_16, " +
			"CASE WHEN match(`dashboard_name`) against ('$keyword' IN BOOLEAN MODE) THEN 9 ELSE 0 END rank_17, " +
			"CASE WHEN match(`dashboard_name`) against ('$keyword' IN BOOLEAN MODE) THEN 3 ELSE 0 END rank_18, " +
			"CASE WHEN match(`dashboard_name`) against ('$keyword*' IN BOOLEAN MODE) THEN 2 ELSE 0 END rank_19, " +
			"CASE WHEN match(`dashboard_name`) against ('*$keyword*' IN BOOLEAN MODE) THEN 1 ELSE 0 END rank_20 " +
			"FROM dict_business_metric WHERE " +
			"MATCH(`metric_name`, `dashboard_name`,  `metric_group`, `metric_category`) " +
			"AGAINST ('*$keyword*' IN BOOLEAN MODE) ) m ORDER BY " +
			"rank DESC, `metric_name`, `metric_category`, `metric_group`, `dashboard_name` LIMIT ?, ?;";

	public final static String SEARCH_DATASET_BY_COMMENTS_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, source, urn, `schema` FROM dict_dataset WHERE id in " +
			"(SELECT dataset_id FROM comments WHERE MATCH(text) against ('*$keyword*' in BOOLEAN MODE) ) " +
			"UNION ALL SELECT id, name, source, urn, `schema` FROM dict_dataset WHERE id in " +
			"(SELECT fd.dataset_id FROM ( " +
			"SELECT id FROM field_comments fc WHERE " +
			"MATCH(comment) AGAINST ('*$keyword*' IN BOOLEAN MODE) ) c JOIN dict_field_detail fd " +
			"ON ( find_in_set(c.id, fd.comment_ids) or c.id = fd.default_comment_id )) " +
			"ORDER BY 2 LIMIT ?, ?;";

	public final static String SEARCH_AUTOCOMPLETE_LIST = "searchSource";

	public final static String GET_METRIC_AUTO_COMPLETE_LIST = "SELECT DISTINCT CASE " +
			"WHEN parent_path is null or parent_path = '' THEN field_name " +
			"ELSE CONCAT_WS('.', parent_path,  field_name) END as full_name FROM dict_field_detail";

	public final static String GET_DATASET_AUTO_COMPLETE_LIST = "SELECT DISTINCT name FROM dict_dataset";

	public final static String GET_FLOW_AUTO_COMPLETE_LIST = "SELECT DISTINCT flow_name FROM flow";

	public final static String GET_JOB_AUTO_COMPLETE_LIST = "SELECT DISTINCT job_name FROM flow_job";

	public static List<String> getAutoCompleteList()
	{
		List<String> cachedAutoCompleteList = (List<String>)Cache.get(SEARCH_AUTOCOMPLETE_LIST);
		if (cachedAutoCompleteList == null || cachedAutoCompleteList.size() == 0)
		{
			//List<String> metricList = getJdbcTemplate().queryForList(GET_METRIC_AUTO_COMPLETE_LIST, String.class);
			List<String> flowList = getJdbcTemplate().queryForList(GET_FLOW_AUTO_COMPLETE_LIST, String.class);
			List<String> jobList = getJdbcTemplate().queryForList(GET_JOB_AUTO_COMPLETE_LIST, String.class);
			List<String> datasetList = getJdbcTemplate().queryForList(GET_DATASET_AUTO_COMPLETE_LIST, String.class);
			cachedAutoCompleteList =
					Stream.concat(datasetList.stream(),
							Stream.concat(flowList.stream(), jobList.stream())).collect(Collectors.toList());
			Collections.sort(cachedAutoCompleteList);
			Cache.set(SEARCH_AUTOCOMPLETE_LIST, cachedAutoCompleteList, 60*60);
		}


		return cachedAutoCompleteList;
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

		try
		{
			keywordNode = utils.Search.generateElasticSearchQueryString(category, source, keywords);
		}
		catch(Exception e)
		{
			Logger.error("Elastic search dataset input query is not JSON format. Error message :" + e.getMessage());
		}

		if (keywordNode != null)
		{
			ObjectNode funcScoreNodes = Json.newObject();

			ObjectNode fieldValueFactorNode = Json.newObject();
			fieldValueFactorNode.put("field","static_boosting_score");
			fieldValueFactorNode.put("factor",1);
			fieldValueFactorNode.put("modifier","square");
			fieldValueFactorNode.put("missing",1);

			funcScoreNodes.put("query", keywordNode);
			funcScoreNodes.put("field_value_factor",fieldValueFactorNode);

			ObjectNode funcScoreNodesWrapper = Json.newObject();
			funcScoreNodesWrapper.put("function_score",funcScoreNodes);

			queryNode.put("query",funcScoreNodesWrapper);

			Logger.debug("The query sent to Elastic Search is: " + queryNode.toString());

			Promise<WSResponse> responsePromise = WS.url(Play.application().configuration().getString(
					SearchDAO.ELASTICSEARCH_DATASET_URL_KEY)).post(queryNode);
			responseNode = responsePromise.get(1000).asJson();

			Logger.debug("The responseNode from Elastic Search is: " + responseNode.toString());

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

				for (Map row : rows) {

					Dataset ds = new Dataset();
					ds.id = (Long)row.get(DatasetRowMapper.DATASET_ID_COLUMN);
					ds.name = (String)row.get(DatasetRowMapper.DATASET_NAME_COLUMN);
					ds.source = (String)row.get(DatasetRowMapper.DATASET_SOURCE_COLUMN);
					ds.urn = (String)row.get(DatasetRowMapper.DATASET_URN_COLUMN);
					ds.schema = (String)row.get(DatasetRowMapper.DATASET_SCHEMA_COLUMN);
					pagedDatasets.add(ds);
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
		List<Metric> pagedMetrics = new ArrayList<Metric>();
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
				List<Map<String, Object>> rows = null;
				rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
				for (Map row : rows) {

					Metric metric = new Metric();
					metric.id = (Integer)row.get(MetricRowMapper.METRIC_ID_COLUMN);
					metric.name = (String)row.get(MetricRowMapper.METRIC_NAME_COLUMN);
					metric.refID = (String)row.get(MetricRowMapper.METRIC_REF_ID_COLUMN);
					metric.refIDType = (String)row.get(MetricRowMapper.METRIC_REF_ID_TYPE_COLUMN);
					metric.description = (String)row.get(MetricRowMapper.METRIC_DESCRIPTION_COLUMN);
					metric.dashboardName = (String)row.get(MetricRowMapper.METRIC_DASHBOARD_NAME_COLUMN);
					metric.category = (String)row.get(MetricRowMapper.METRIC_CATEGORY_COLUMN);
					metric.group = (String)row.get(MetricRowMapper.METRIC_GROUP_COLUMN);
					metric.source = "metric";
					metric.urn = "";
					if (StringUtils.isNotBlank(metric.dashboardName))
					{
						metric.urn += metric.dashboardName + "/";
					}
					if (StringUtils.isNotBlank(metric.group))
					{
						metric.urn += metric.group + "/";
					}
					if (StringUtils.isNotBlank(metric.name))
					{
						metric.urn += metric.name;
					}

					ObjectNode schema = Json.newObject();
					schema.put(MetricRowMapper.METRIC_REF_ID_COLUMN, metric.refID);
					schema.put(MetricRowMapper.METRIC_REF_ID_TYPE_COLUMN, metric.refIDType);
					schema.put(MetricRowMapper.METRIC_DESCRIPTION_COLUMN, metric.description);
					schema.put(MetricRowMapper.METRIC_DASHBOARD_NAME_COLUMN, metric.dashboardName);
					schema.put(MetricRowMapper.METRIC_CATEGORY_COLUMN, metric.category);
					schema.put(MetricRowMapper.METRIC_GROUP_COLUMN, metric.group);
					metric.schema = schema.toString();
					pagedMetrics.add(metric);
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

				rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
				for (Map row : rows) {

					FlowJob flow = new FlowJob();
					flow.flowId = (Long)row.get(FlowRowMapper.FLOW_ID_COLUMN);
					flow.flowName = (String)row.get(FlowRowMapper.FLOW_NAME_COLUMN);
					flow.flowPath = (String)row.get(FlowRowMapper.FLOW_PATH_COLUMN);
					flow.flowGroup = (String)row.get(FlowRowMapper.FLOW_GROUP_COLUMN);
					flow.appCode = (String)row.get(FlowRowMapper.APP_CODE_COLUMN);
					flow.appId = (Integer)row.get(FlowRowMapper.APP_ID_COLUMN);
					flow.displayName = flow.flowName;
					flow.link = "#/flows/name/" + flow.appCode + "/" +
							Long.toString(flow.flowId) + "/page/1?urn=" + flow.flowGroup;
					flow.path = flow.appCode + "/" + flow.flowPath;
					pagedFlows.add(flow);
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

				rows = jdbcTemplate.queryForList(query, (page-1)*size, size);
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
					flowJob.link = "#/flows/name/" + flowJob.appCode + "/" +
							Long.toString(flowJob.flowId) + "/page/1?urn=" + flowJob.flowGroup;
					flowJob.path = flowJob.appCode + "/" + flowJob.jobPath;

					pagedFlowJobs.add(flowJob);
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
				String query = SEARCH_DATASET_BY_COMMENTS_WITH_PAGINATION.replace("$keyword", keyword);
				rows = jdbcTemplate.queryForList(query, (page-1)*size, size);

				for (Map row : rows) {

					Dataset ds = new Dataset();
					ds.id = (long)row.get(DatasetRowMapper.DATASET_ID_COLUMN);
					ds.name = (String)row.get(DatasetRowMapper.DATASET_NAME_COLUMN);
					ds.source = (String)row.get(DatasetRowMapper.DATASET_SOURCE_COLUMN);
					ds.urn = (String)row.get(DatasetRowMapper.DATASET_URN_COLUMN);
					ds.schema = (String)row.get(DatasetRowMapper.DATASET_SCHEMA_COLUMN);
					pagedDatasets.add(ds);
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
