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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.Dataset;
import models.FlowJob;
import models.Metric;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.Play;
import play.libs.F.Promise;
import play.libs.Json;
import play.libs.ws.*;

import java.util.*;

public class AdvSearchDAO extends AbstractMySQLOpenSourceDAO
{
	public final static String GET_DATASET_SOURCES = "SELECT source " +
			"FROM dict_dataset GROUP BY 1 ORDER BY count(*) DESC";

	public final static String GET_FLOW_APPCODES = "SELECT DISTINCT app_code " +
			"FROM cfg_application GROUP BY 1 ORDER BY 1";

	public final static String GET_DATASET_SCOPES = "SELECT DISTINCT parent_name " +
			"FROM dict_dataset WHERE parent_name is not null order by 1;";

	public final static String GET_DATASET_TABLE_NAMES_BY_SCOPE = "SELECT DISTINCT name " +
			"FROM dict_dataset WHERE parent_name in (:scopes)";

	public final static String GET_FLOW_NAMES_BY_APP = "SELECT DISTINCT f.flow_name " +
			"FROM flow f JOIN cfg_application a on f.app_id = a.app_id WHERE app in (:apps)";

	public final static String GET_DATASET_TABLE_NAMES = "SELECT DISTINCT name FROM dict_dataset ORDER BY 1";

	public final static String GET_FLOW_NAMES = "SELECT DISTINCT flow_name FROM flow ORDER BY 1";

	public final static String GET_JOB_NAMES = "SELECT DISTINCT job_name " +
			"FROM flow_job GROUP BY 1 ORDER BY 1";

	public final static String GET_DATASET_FIELDS = "SELECT DISTINCT field_name " +
			"FROM dict_field_detail ORDER BY 1";

	public final static String GET_DATASET_FIELDS_BY_TABLE_NAMES = "SELECT DISTINCT f.field_name " +
			"FROM dict_field_detail f join dict_dataset d on f.dataset_id = d.id where d.name regexp";

	public final static String SEARCH_DATASETS_BY_COMMENTS_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, source, urn, `schema` FROM dict_dataset where id in ( " +
			"SELECT dataset_id FROM comments WHERE MATCH(text) against ('*$keyword*' in BOOLEAN MODE) ) " +
			"UNION ALL SELECT id, name, source, urn, `schema` from dict_dataset " +
			"WHERE id in ( SELECT DISTINCT dataset_id FROM " +
			"dict_dataset_field_comment WHERE comment_id in " +
			"(SELECT id FROM field_comments where MATCH(comment) against ('*$keyword*' in BOOLEAN MODE))) " +
			"ORDER BY 2 LIMIT ?, ?";

	public final static String ADVSEARCH_RANK_CLAUSE = " ORDER BY CASE WHEN $condition1 THEN 0 " +
			"WHEN $condition2 THEN 2 WHEN $condition3 THEN 3 WHEN $condition4 THEN 4 ELSE 9 END, " +
			"CASE WHEN urn LIKE 'teradata://DWH_%' THEN 2 WHEN urn LIKE 'hdfs://data/tracking/%' THEN 1 " +
			"WHEN urn LIKE 'teradata://DWH/%' THEN 3 WHEN urn LIKE 'hdfs://data/databases/%' THEN 4 " +
			"WHEN urn LIKE 'hdfs://data/dervied/%' THEN 5 ELSE 99 END, urn";

	public final static String DATASET_BY_COMMENT_PAGINATION_IN_CLAUSE = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, source, `schema`, urn, FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM dict_dataset WHERE id IN ( " +
			"SELECT dataset_id FROM comments WHERE MATCH(text) " +
			"AGAINST ('*$keyword*' in BOOLEAN MODE) and dataset_id in ($id_list)) " +
			"UNION ALL SELECT id, name, source, `schema`, urn, FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM dict_dataset WHERE id IN (SELECT DISTINCT dataset_id FROM " +
			"dict_dataset_field_comment WHERE comment_id in " +
			"(SELECT id FROM field_comments where MATCH(comment) against ('*$keyword*' in BOOLEAN MODE)) ) " +
			"ORDER BY 2 LIMIT ?, ?;";

	public final static String ADV_SEARCH_FLOW = "SELECT SQL_CALC_FOUND_ROWS " +
			"a.app_code, f.flow_id, f.flow_name, f.flow_path, f.flow_group FROM flow f " +
			"JOIN cfg_application a on f.app_id = a.app_id ";

	public final static String ADV_SEARCH_JOB = "SELECT SQL_CALC_FOUND_ROWS " +
			"a.app_code, f.flow_name, f.flow_path, f.flow_group, j.flow_id, j.job_id, " +
			"j.job_name, j.job_path, j.job_type " +
			"FROM flow_job j JOIN flow f on j.app_id = f.app_id  AND j.flow_id = f.flow_id " +
			"JOIN cfg_application a on j.app_id = a.app_id ";

	public final static String ADV_SEARCH_METRIC = "SELECT SQL_CALC_FOUND_ROWS metric_id, " +
			"metric_name, metric_description, dashboard_name, metric_group, metric_category, " +
			"metric_sub_category, metric_level, metric_source_type, metric_source, " +
			"metric_source_dataset_id, metric_ref_id_type, metric_ref_id, metric_type, metric_grain, " +
			"metric_display_factor, metric_display_factor_sym, metric_good_direction, " +
			"metric_formula, dimensions, owners, tags, urn, metric_url, wiki_url, scm_url, 0 as watch_id " +
			"FROM dict_business_metric ";

	public static List<String> getDatasetSources()
	{
		return getJdbcTemplate().queryForList(GET_DATASET_SOURCES, String.class);
	}

	public static List<String> getDatasetScopes()
	{
		return getJdbcTemplate().queryForList(GET_DATASET_SCOPES, String.class);
	}

	public static List<String> getTableNames(String scopes)
	{
		List<String> tables = null;
		if (StringUtils.isNotBlank(scopes))
		{
			String[] scopeArray = scopes.split(",");
			List<String> scopeList = Arrays.asList(scopeArray);
			Map<String, List> param = Collections.singletonMap("scopes", scopeList);
			NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
					NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
			tables = namedParameterJdbcTemplate.queryForList(
					GET_DATASET_TABLE_NAMES_BY_SCOPE, param, String.class);
		}
		else
		{
			tables = getJdbcTemplate().queryForList(GET_DATASET_TABLE_NAMES, String.class);
		}

		return tables;
	}

	public static List<String> getFields(String tables)
	{
		String query = null;
		if (StringUtils.isNotBlank(tables))
		{
			String[] tableArray = tables.split(",");
			query = GET_DATASET_FIELDS_BY_TABLE_NAMES;
			query += "'";
			for(int i = 0; i < tableArray.length; i++)
			{
				if (i == 0)
				{
					query += tableArray[i];
				}
				else
				{
					query += "|" + tableArray[i];
				}
			}
			query += "' order by 1";
		}
		else
		{
			query = GET_DATASET_FIELDS;
		}

		return getJdbcTemplate().queryForList(query, String.class);
	}

	public static List<String> getFlowApplicationCodes()
	{
		return getJdbcTemplate().queryForList(GET_FLOW_APPCODES, String.class);
	}

	public static List<String> getFlowNames(String applications)
	{
		List<String> flowNames = null;
		if (StringUtils.isNotBlank(applications))
		{
			String[] appArray = applications.split(",");
			List<String> appList = Arrays.asList(appArray);
			Map<String, List> param = Collections.singletonMap("apps", appList);
			NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
					NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
			flowNames = namedParameterJdbcTemplate.queryForList(
					GET_FLOW_NAMES_BY_APP, param, String.class);
		}
		else
		{
			flowNames = getJdbcTemplate().queryForList(GET_FLOW_NAMES, String.class);
		}

		return flowNames;
	}

	public static List<String> getFlowJobNames()
	{
		return getJdbcTemplate().queryForList(GET_JOB_NAMES, String.class);
	}

	public static ObjectNode elasticSearch(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<Dataset> pagedDatasets = new ArrayList<>();
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);

		JsonNode searchNode = utils.Search.generateDatasetAdvSearchQueryString(searchOpt);

		if (searchNode != null && searchNode.isContainerNode())
		{
			queryNode.set("query", searchNode);
		}
		Promise<WSResponse> responsePromise = WS.url(
				Play.application().configuration().getString(
						SearchDAO.ELASTICSEARCH_DATASET_URL_KEY)).post(queryNode);
		JsonNode responseNode = responsePromise.get(1000).asJson();

		resultNode.put("page", page);
		resultNode.put("category", "Datasets");
		resultNode.put("itemsPerPage", size);

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
		resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
		resultNode.set("data", Json.toJson(pagedDatasets));
		return resultNode;
	}

	public static ObjectNode elasticSearchMetric(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<Metric> pagedMetrics = new ArrayList<>();
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);

		JsonNode searchNode = utils.Search.generateMetricAdvSearchQueryString(searchOpt);

		if (searchNode != null && searchNode.isContainerNode())
		{
			queryNode.set("query", searchNode);
		}

		Promise<WSResponse> responsePromise = WS.url(Play.application().configuration().getString(
				SearchDAO.ELASTICSEARCH_METRIC_URL_KEY)).post(queryNode);
		JsonNode responseNode = responsePromise.get(1000).asJson();

		resultNode.put("page", page);
		resultNode.put("category", "Metrics");
		resultNode.put("isMetrics", true);
		resultNode.put("itemsPerPage", size);

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

	public static ObjectNode elasticSearchFlowJobs(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		Long count = 0L;
		List<FlowJob> pagedFlows = new ArrayList<>();
		ObjectNode queryNode = Json.newObject();
		queryNode.put("from", (page-1)*size);
		queryNode.put("size", size);

		JsonNode searchNode = utils.Search.generateFlowJobAdvSearchQueryString(searchOpt);

		if (searchNode != null && searchNode.isContainerNode())
		{
			queryNode.set("query", searchNode);
		}

		Promise<WSResponse> responsePromise = WS.url(Play.application().configuration().getString(
				SearchDAO.ELASTICSEARCH_FLOW_URL_KEY)).post(queryNode);
		JsonNode responseNode = responsePromise.get(1000).asJson();

		resultNode.put("page", page);
		resultNode.put("category", "Flows");
		resultNode.put("isFlowJob", true);
		resultNode.put("itemsPerPage", size);

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
									pagedFlows.add(flowJob);
								}
							}
						}
					}
				}
			}
		}
		resultNode.put("count", count);
		resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
		resultNode.set("data", Json.toJson(pagedFlows));
		return resultNode;
	}

	public static ObjectNode search(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		int count = 0;
		List<String> scopeInList = new ArrayList<String>();
		List<String> scopeNotInList = new ArrayList<String>();
		List<String> tableInList = new ArrayList<String>();
		List<String> tableNotInList = new ArrayList<String>();
		List<String> fieldAnyList = new ArrayList<String>();
		List<String> fieldAllList = new ArrayList<String>();
		List<String> fieldNotInList = new ArrayList<String>();
		String fieldAllIDs = "";
		String comments = "";

		if (searchOpt != null && (searchOpt.isContainerNode()))
		{
			if (searchOpt.has("scope")) {
				JsonNode scopeNode = searchOpt.get("scope");
				if (scopeNode != null && scopeNode.isContainerNode())
				{
					if (scopeNode.has("in"))
					{
						JsonNode scopeInNode = scopeNode.get("in");
						if (scopeInNode != null)
						{
							String scopeInStr = scopeInNode.asText();
							if (StringUtils.isNotBlank(scopeInStr))
							{
								String[] scopeInArray = scopeInStr.split(",");
								if (scopeInArray != null)
								{
									for(String value : scopeInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											scopeInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (scopeNode.has("not"))
					{
						JsonNode scopeNotInNode = scopeNode.get("not");
						if (scopeNotInNode != null)
						{
							String scopeNotInStr = scopeNotInNode.asText();
							if (StringUtils.isNotBlank(scopeNotInStr))
							{
								String[] scopeNotInArray = scopeNotInStr.split(",");
								if (scopeNotInArray != null)
								{
									for(String value : scopeNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											scopeNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("table")) {
				JsonNode tableNode = searchOpt.get("table");
				if (tableNode != null && tableNode.isContainerNode())
				{
					if (tableNode.has("in"))
					{
						JsonNode tableInNode = tableNode.get("in");
						if (tableInNode != null)
						{
							String tableInStr = tableInNode.asText();
							if (StringUtils.isNotBlank(tableInStr))
							{
								String[] tableInArray = tableInStr.split(",");
								if (tableInArray != null)
								{
									for(String value : tableInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											tableInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (tableNode.has("not"))
					{
						JsonNode tableNotInNode = tableNode.get("not");
						if (tableNotInNode != null)
						{
							String tableNotInStr = tableNotInNode.asText();
							if (StringUtils.isNotBlank(tableNotInStr))
							{
								String[] tableNotInArray = tableNotInStr.split(",");
								if (tableNotInArray != null)
								{
									for(String value : tableNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											tableNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("fields")) {
				JsonNode fieldNode = searchOpt.get("fields");
				if (fieldNode != null && fieldNode.isContainerNode())
				{
					if (fieldNode.has("any"))
					{
						JsonNode fieldAnyNode = fieldNode.get("any");
						if (fieldAnyNode != null)
						{
							String fieldAnyStr = fieldAnyNode.asText();
							if (StringUtils.isNotBlank(fieldAnyStr))
							{
								String[] fieldAnyArray = fieldAnyStr.split(",");
								if (fieldAnyArray != null)
								{
									for(String value : fieldAnyArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											fieldAnyList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (fieldNode.has("all"))
					{
						JsonNode fieldAllNode = fieldNode.get("all");
						if (fieldAllNode != null)
						{
							String fieldAllStr = fieldAllNode.asText();
							if (StringUtils.isNotBlank(fieldAllStr))
							{
								String[] fieldAllArray = fieldAllStr.split(",");
								if (fieldAllArray != null)
								{
									for(String value : fieldAllArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											fieldAllList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (fieldNode.has("not"))
					{
						JsonNode fieldNotInNode = fieldNode.get("not");
						if (fieldNotInNode != null)
						{
							String fieldNotInStr = fieldNotInNode.asText();
							if (StringUtils.isNotBlank(fieldNotInStr))
							{
								String[] fieldNotInArray = fieldNotInStr.split(",");
								if (fieldNotInArray != null)
								{
									for(String value : fieldNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											fieldNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			String datasetSources = "";
			if (searchOpt.has("sources")) {
				JsonNode sourcesNode = searchOpt.get("sources");
				if (sourcesNode != null)
				{
					datasetSources = sourcesNode.asText();
				}
			}

			boolean needAndKeyword = false;
			int fieldQueryIndex = 0;
			if (fieldAllList.size() > 0)
			{
				String fieldAllQuery = "SELECT DISTINCT f1.dataset_id FROM dict_field_detail f1 ";
				String fieldWhereClause = " WHERE ";
				for (String field : fieldAllList)
				{
					fieldQueryIndex++;
					if (fieldQueryIndex == 1)
					{
						fieldWhereClause += "f1.field_name LIKE '%" + field + "%' ";
					}
					else
					{
						fieldAllQuery += "JOIN dict_field_detail f" + fieldQueryIndex + " ON f" +
								(fieldQueryIndex-1) + ".dataset_id = f" + fieldQueryIndex + ".dataset_id ";
						fieldWhereClause += " and f" + fieldQueryIndex + ".field_name LIKE '%" + field + "%' ";

					}
				}
				fieldAllQuery += fieldWhereClause;
				List<Map<String, Object>> rows = getJdbcTemplate().queryForList(fieldAllQuery);
				for (Map row : rows) {

					fieldAllIDs += (Long)row.get("dataset_id") + ",";
				}
				if (fieldAllIDs.length() > 0)
				{
					fieldAllIDs = fieldAllIDs.substring(0, fieldAllIDs.length()-1);
				}
				if (StringUtils.isBlank(fieldAllIDs))
				{
					fieldAllIDs = Integer.toString(0);

				}
			}

			List<Dataset> pagedDatasets = new ArrayList<Dataset>();
			final JdbcTemplate jdbcTemplate = getJdbcTemplate();
			javax.sql.DataSource ds = jdbcTemplate.getDataSource();
			DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

			TransactionTemplate txTemplate = new TransactionTemplate(tm);

			ObjectNode result;

			if (searchOpt.has("comments"))
			{
				JsonNode commentsNode = searchOpt.get("comments");
				if (commentsNode != null)
				{
					comments = commentsNode.asText();
					if (StringUtils.isNotBlank(comments))
					{
						if (scopeInList.size() == 0 && scopeNotInList.size() == 0
								&& tableInList.size() == 0 && tableNotInList.size() == 0
								&& fieldAllList.size() == 0 && fieldAnyList.size() == 0 && fieldNotInList.size() == 0)
						{
							final String commentsQueryStr =
									SEARCH_DATASETS_BY_COMMENTS_WITH_PAGINATION.replace("$keyword", comments);

							result = txTemplate.execute(new TransactionCallback<ObjectNode>()
							{
								public ObjectNode doInTransaction(TransactionStatus status)
								{
									List<Map<String, Object>> rows = null;
									rows = jdbcTemplate.queryForList(commentsQueryStr, (page-1)*size, size);

									for (Map row : rows) {

										Dataset ds = new Dataset();
										ds.id = (Long)row.get("id");
										ds.name = (String)row.get("name");
										ds.source = (String)row.get("source");
										ds.urn = (String)row.get("urn");
										ds.schema = (String)row.get("schema");
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
									resultNode.put("itemsPerPage", size);
									resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
									resultNode.set("data", Json.toJson(pagedDatasets));

									return resultNode;
								}
							});
							return result;
						}
					}
				}
			}

			String query = "";
			if (StringUtils.isNotBlank(comments))
			{
				query = "SELECT DISTINCT d.id FROM dict_dataset d";
			}
			else
			{
				query = "SELECT SQL_CALC_FOUND_ROWS " +
						"DISTINCT d.id, d.name, d.schema, d.source, d.urn, " +
						"FROM_UNIXTIME(d.source_modified_time) as modified FROM dict_dataset d";
			}
			if (fieldAllList.size() > 0 || fieldAnyList.size() > 0 || fieldNotInList.size() > 0)
			{
				String fieldQuery = "SELECT DISTINCT dataset_id FROM dict_field_detail f WHERE (";
				query += " WHERE d.id IN ( ";
				query += fieldQuery;
				String whereClause = "";
				boolean fieldNeedAndKeyword = false;
				if (fieldAnyList.size() > 0)
				{
					whereClause = " (";
					int indexForAnyList = 0;
					for (String field : fieldAnyList)
					{
						if (indexForAnyList == 0)
						{
							whereClause += "f.field_name LIKE '%" + field + "%'";
						}
						else
						{
							whereClause += " or f.field_name LIKE '%" + field + "%'";
						}
						indexForAnyList++;
					}
					whereClause += " ) ";
					fieldNeedAndKeyword = true;
					query += whereClause;
				}
				if (fieldAllList.size() > 0 && StringUtils.isNotBlank(fieldAllIDs))
				{
					if (fieldNeedAndKeyword)
					{
						whereClause = " and (";
					}
					else
					{
						whereClause = " (";
					}
					whereClause += "f.dataset_id IN (" + fieldAllIDs + ")";
					whereClause += " ) ";
					query += whereClause;
					fieldNeedAndKeyword = true;
				}
				if (fieldNotInList.size() > 0)
				{
					if (fieldNeedAndKeyword)
					{
						whereClause = " and ( f.dataset_id not in (select dataset_id from dict_field_detail where";
					}
					else
					{
						whereClause = " ( f.dataset_id not in (select dataset_id from dict_field_detail where";
					}
					int indexForNotInList = 0;
					for (String field : fieldNotInList)
					{
						if (indexForNotInList == 0)
						{
							whereClause += " field_name LIKE '%" + field + "%'";
						}
						else
						{
							whereClause += " or field_name LIKE '%" + field + "%'";
						}
						indexForNotInList++;
					}
					whereClause += " )) ";
					query += whereClause;
					fieldNeedAndKeyword = true;
				}
				needAndKeyword = true;
				query += ") )";
			}

			if (scopeInList.size() > 0 || scopeNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " and";
				}
				else
				{
					query += " where";
				}
				boolean scopeNeedAndKeyword = false;
				if (scopeInList.size() > 0)
				{
					query += " d.parent_name in (";
					scopeNeedAndKeyword = true;
					int indexForScopeInList = 0;
					for (String scope : scopeInList)
					{
						if (indexForScopeInList == 0)
						{
							query += "'" + scope + "'";
						}
						else
						{
							query += ", '" + scope + "'";
						}
						indexForScopeInList++;
					}
					query += ") ";
				}
				if (scopeNotInList.size() > 0)
				{
					if (scopeNeedAndKeyword)
					{
						query += " and d.parent_name not in (";
					}
					else
					{
						query += " d.parent_name not in (";
					}
					int indexForScopeNotInList = 0;
					for (String scope : scopeNotInList)
					{
						if (indexForScopeNotInList == 0)
						{
							query += "'" + scope + "'";
						}
						else
						{
							query += ", '" + scope + "'";
						}
						indexForScopeNotInList++;
					}
					query += ") ";
				}
				needAndKeyword = true;
			}
			String condition1 = "";
			String condition2 = "";
			String condition3 = "";
			String condition4 = "";

			if (tableInList.size() > 0 || tableNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " and";
				}
				else
				{
					query += " where";
				}
				boolean tableNeedAndKeyword = false;
				if (tableInList.size() > 0)
				{
					query += " (";
					int indexForTableInList = 0;
					for (String table : tableInList)
					{
						if (indexForTableInList == 0)
						{
							query += "d.name LIKE '%" + table + "%'";
						}
						else
						{
							condition1 += " or ";
							condition2 += " or ";
							condition3 += " or ";
							condition4 += " or ";
							query += " or d.name LIKE '%" + table + "%'";
						}
						condition1 += "name = '" + table + "'";
						condition2 += "name LIKE '" + table + "%'";
						condition3 += "name LIKE '%" + table + "'";
						condition4 += "name LIKE '%" + table + "%'";
						indexForTableInList++;
					}
					query += " ) ";
					tableNeedAndKeyword = true;
				}
				if (tableNotInList.size() > 0)
				{
					if (tableNeedAndKeyword)
					{
						query += " and (";
					}
					else
					{
						query += " (";
					}
					int indexForTableNotInList = 0;
					for (String table : tableNotInList)
					{
						if (indexForTableNotInList == 0)
						{
							query += "d.name NOT LIKE '%" + table + "%'";
						}
						else
						{
							query += " and d.name NOT LIKE '%" + table + "%'";
						}
						indexForTableNotInList++;
					}
					query += " ) ";
				}
				needAndKeyword = true;
			}

			if (StringUtils.isNotBlank(datasetSources))
			{
				if (needAndKeyword)
				{
					query += " and";
				}
				else
				{
					query += " WHERE";
				}
				query += " d.source in (";
				String[] dataestSourceArray = datasetSources.split(",");
				for(int i = 0; i < dataestSourceArray.length; i++)
				{
					query += "'" + dataestSourceArray[i] + "'";
					if (i != (dataestSourceArray.length -1))
					{
						query += ",";
					}
				}
				query += ")";
			}
			if ((tableInList.size() > 0 || tableNotInList.size() > 0) &&
					StringUtils.isNotBlank(condition1) &&
					StringUtils.isNotBlank(condition2) &&
					StringUtils.isNotBlank(condition3) &&
					StringUtils.isNotBlank(condition4))
			{
				query += ADVSEARCH_RANK_CLAUSE.replace("$condition1", condition1)
						.replace("$condition2", condition2)
						.replace("$condition3", condition3)
						.replace("$condition4", condition4);
			}
			else
			{
				query += " ORDER BY CASE WHEN urn LIKE 'teradata://DWH_%' THEN 2 " +
						"WHEN urn LIKE 'hdfs://data/tracking/%' THEN 1 " +
						"WHEN urn LIKE 'teradata://DWH/%' THEN 3 " +
						"WHEN urn LIKE 'hdfs://data/databases/%' THEN 4 " +
						"WHEN urn LIKE 'hdfs://data/dervied/%' THEN 5 ELSE 99 end, urn";
			}
			if (StringUtils.isBlank(comments))
			{
				query += " LIMIT " + (page-1)*size + ", " + size;
				final String queryString = query;

				result = txTemplate.execute(new TransactionCallback<ObjectNode>()
				{
					public ObjectNode doInTransaction(TransactionStatus status)
					{
						List<Map<String, Object>> rows = null;
						rows = jdbcTemplate.queryForList(queryString);

						for (Map row : rows) {

							Dataset ds = new Dataset();
							ds.id = (Long)row.get("id");
							ds.name = (String)row.get("name");
							ds.source = (String)row.get("source");
							ds.urn = (String)row.get("urn");
							ds.schema = (String)row.get("schema");
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
						resultNode.put("itemsPerPage", size);
						resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
						resultNode.set("data", Json.toJson(pagedDatasets));

						return resultNode;
					}
				});
				return result;
			}
			else
			{
				String datasetIDStr = "";
				final String queryString = query;

				datasetIDStr = txTemplate.execute(new TransactionCallback<String>()
				{
					public String doInTransaction(TransactionStatus status)
					{
						List<Map<String, Object>> rows = null;
						rows = jdbcTemplate.queryForList(queryString);
						String idsString = "";

						for (Map row : rows) {

							Long id = (Long)row.get("id");
							idsString += Long.toString(id) + ",";
						}
						if (StringUtils.isNotBlank(idsString))
						{
							idsString = idsString.substring(0, idsString.length()-1);
						}
						return idsString;
					}
				});
				if (StringUtils.isBlank(datasetIDStr))
				{
					resultNode.put("count", 0);
					resultNode.put("page", page);
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", 0);
					resultNode.set("data", Json.toJson(""));
					return resultNode;
				}
				final String commentsQueryWithConditionStr = DATASET_BY_COMMENT_PAGINATION_IN_CLAUSE.replace("$keyword", comments).
						replace("$id_list", datasetIDStr);
				result = txTemplate.execute(new TransactionCallback<ObjectNode>()
				{
					public ObjectNode doInTransaction(TransactionStatus status)
					{
						List<Map<String, Object>> rows = null;
						rows = jdbcTemplate.queryForList(commentsQueryWithConditionStr, (page-1)*size, size);

						for (Map row : rows) {

							Dataset ds = new Dataset();
							ds.id = (Long)row.get("id");
							ds.name = (String)row.get("name");
							ds.source = (String)row.get("source");
							ds.urn = (String)row.get("urn");
							ds.schema = (String)row.get("schema");
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
						resultNode.put("itemsPerPage", size);
						resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
						resultNode.set("data", Json.toJson(pagedDatasets));

						return resultNode;
					}
				});
				return result;
			}
		}
		resultNode.put("count", 0);
		resultNode.put("page", page);
		resultNode.put("itemsPerPage", size);
		resultNode.put("totalPages", 0);
		resultNode.set("data", Json.toJson(""));
		return resultNode;
	}

	public static ObjectNode searchFlows(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		int count = 0;
		List<String> appcodeInList = new ArrayList<String>();
		List<String> appcodeNotInList = new ArrayList<String>();
		List<String> flowInList = new ArrayList<String>();
		List<String> flowNotInList = new ArrayList<String>();
		List<String> jobInList = new ArrayList<String>();
		List<String> jobNotInList = new ArrayList<String>();

		if (searchOpt != null && (searchOpt.isContainerNode()))
		{
			if (searchOpt.has("appcode")) {
				JsonNode appcodeNode = searchOpt.get("appcode");
				if (appcodeNode != null && appcodeNode.isContainerNode())
				{
					if (appcodeNode.has("in"))
					{
						JsonNode appcodeInNode = appcodeNode.get("in");
						if (appcodeInNode != null)
						{
							String appcodeInStr = appcodeInNode.asText();
							if (StringUtils.isNotBlank(appcodeInStr))
							{
								String[] appcodeInArray = appcodeInStr.split(",");
								if (appcodeInArray != null)
								{
									for(String value : appcodeInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											appcodeInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (appcodeNode.has("not"))
					{
						JsonNode appcodeNotInNode = appcodeNode.get("not");
						if (appcodeNotInNode != null)
						{
							String appcodeNotInStr = appcodeNotInNode.asText();
							if (StringUtils.isNotBlank(appcodeNotInStr))
							{
								String[] appcodeNotInArray = appcodeNotInStr.split(",");
								if (appcodeNotInArray != null)
								{
									for(String value : appcodeNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											appcodeNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("flow")) {
				JsonNode flowNode = searchOpt.get("flow");
				if (flowNode != null && flowNode.isContainerNode())
				{
					if (flowNode.has("in"))
					{
						JsonNode flowInNode = flowNode.get("in");
						if (flowInNode != null)
						{
							String flowInStr = flowInNode.asText();
							if (StringUtils.isNotBlank(flowInStr))
							{
								String[] flowInArray = flowInStr.split(",");
								if (flowInArray != null)
								{
									for(String value : flowInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											flowInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (flowNode.has("not"))
					{
						JsonNode flowNotInNode = flowNode.get("not");
						if (flowNotInNode != null)
						{
							String flowNotInStr = flowNotInNode.asText();
							if (StringUtils.isNotBlank(flowNotInStr))
							{
								String[] flowNotInArray = flowNotInStr.split(",");
								if (flowNotInArray != null)
								{
									for(String value : flowNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											flowNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("job")) {
				JsonNode jobNode = searchOpt.get("job");
				if (jobNode != null && jobNode.isContainerNode())
				{
					if (jobNode.has("in"))
					{
						JsonNode jobInNode = jobNode.get("in");
						if (jobInNode != null)
						{
							String jobInStr = jobInNode.asText();
							if (StringUtils.isNotBlank(jobInStr))
							{
								String[] jobInArray = jobInStr.split(",");
								if (jobInArray != null)
								{
									for(String value : jobInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											jobInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (jobNode.has("not"))
					{
						JsonNode jobNotInNode = jobNode.get("not");
						if (jobNotInNode != null)
						{
							String jobNotInStr = jobNotInNode.asText();
							if (StringUtils.isNotBlank(jobNotInStr))
							{
								String[] jobNotInArray = jobNotInStr.split(",");
								if (jobNotInArray != null)
								{
									for(String value : jobNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											jobNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			boolean needAndKeyword = false;

			final List<FlowJob> pagedFlows = new ArrayList<FlowJob>();
			final JdbcTemplate jdbcTemplate = getJdbcTemplate();
			javax.sql.DataSource ds = jdbcTemplate.getDataSource();
			DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

			TransactionTemplate txTemplate = new TransactionTemplate(tm);

			ObjectNode result;
			String query = null;
			if (jobInList.size() > 0 || jobNotInList.size() > 0)
			{
				query = ADV_SEARCH_JOB;
			}
			else
			{
				query = ADV_SEARCH_FLOW;
			}

			if (appcodeInList.size() > 0 || appcodeNotInList.size() > 0)
			{
				boolean appcodeNeedAndKeyword = false;
				if (appcodeInList.size() > 0)
				{
					int indexForAppcodeInList = 0;
					for (String appcode : appcodeInList)
					{
						if (indexForAppcodeInList == 0)
						{
							query += "WHERE a.app_code in ('" + appcode + "'";
						}
						else
						{
							query += ", '" + appcode + "'";
						}
						indexForAppcodeInList++;
					}
					query += ") ";
					appcodeNeedAndKeyword = true;
				}
				if (appcodeNotInList.size() > 0)
				{
					if (appcodeNeedAndKeyword)
					{
						query += " AND ";
					}
					else
					{
						query += " WHERE ";
					}
					int indexForAppcodeNotInList = 0;
					for (String appcode : appcodeNotInList)
					{
						if (indexForAppcodeNotInList == 0)
						{
							query += "a.app_code not in ('" + appcode + "'";
						}
						else
						{
							query += ", '" + appcode + "'";
						}
						indexForAppcodeNotInList++;
					}
					query += ") ";
				}
				needAndKeyword = true;
			}

			if (flowInList.size() > 0 || flowNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " AND ";
				}
				else
				{
					query += " WHERE ";
				}
				boolean flowNeedAndKeyword = false;
				if (flowInList.size() > 0)
				{
					query += "( ";
					int indexForFlowInList = 0;
					for (String flow : flowInList)
					{
						if (indexForFlowInList == 0)
						{
							query += "f.flow_name LIKE '%" + flow + "%'";
						}
						else
						{
							query += " or f.flow_name LIKE '%" + flow + "%'";
						}
						indexForFlowInList++;
					}
					query += ") ";
					flowNeedAndKeyword = true;
				}
				if (flowNotInList.size() > 0)
				{
					if (flowNeedAndKeyword)
					{
						query += " AND ";
					}
					query += "( ";
					int indexForFlowNotInList = 0;
					for (String flow : flowNotInList)
					{
						if (indexForFlowNotInList == 0)
						{
							query += "f.flow_name NOT LIKE '%" + flow + "%'";
						}
						else
						{
							query += " and f.flow_name NOT LIKE '%" + flow + "%'";
						}
						indexForFlowNotInList++;
					}
					query += ") ";
				}
				needAndKeyword = true;
			}

			if (jobInList.size() > 0 || jobNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " AND ";
				}
				else
				{
					query += " WHERE ";
				}
				query += "( ";
				boolean jobNeedAndKeyword = false;
				if (jobInList.size() > 0)
				{
					query += "( ";
					int indexForJobInList = 0;
					for (String job : jobInList)
					{
						if (indexForJobInList == 0)
						{
							query += "j.job_name LIKE '%" + job + "%'";
						}
						else
						{
							query += " or j.job_name LIKE '%" + job + "%'";
						}
						indexForJobInList++;
					}
					query += ") ";
					jobNeedAndKeyword = true;
				}
				if (jobNotInList.size() > 0)
				{
					if (jobNeedAndKeyword)
					{
						query += " AND ";
					}
					query += "( ";
					int indexForJobNotInList = 0;
					for (String job : jobNotInList)
					{
						if (indexForJobNotInList == 0)
						{
							query += "j.job_name NOT LIKE '%" + job + "%'";
						}
						else
						{
							query += " and j.job_name NOT LIKE '%" + job + "%'";
						}
						indexForJobNotInList++;
					}
					query += ") ";
				}
				query += " ) ";
			}

			query += " LIMIT " + (page-1)*size + ", " + size;
			final String queryString = query;

			result = txTemplate.execute(new TransactionCallback<ObjectNode>()
			{
				public ObjectNode doInTransaction(TransactionStatus status)
				{
					List<Map<String, Object>> rows = null;
					rows = jdbcTemplate.queryForList(queryString);

					for (Map row : rows) {

						FlowJob flow = new FlowJob();
						flow.appCode = (String)row.get("app_code");
						flow.flowName = (String)row.get("flow_name");
						flow.flowPath = (String)row.get("flow_path");
						flow.flowGroup = (String)row.get("flow_group");
						flow.jobName = (String)row.get("job_name");
						flow.jobPath = (String)row.get("job_path");
						flow.flowId = (Long)row.get("flow_id");
						if (StringUtils.isNotBlank(flow.jobName))
						{
							flow.displayName = flow.jobName;
						}
						else
						{
							flow.displayName = flow.flowName;
						}
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
					resultNode.put("page", page);
					resultNode.put("isFlowJob", true);
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
					resultNode.set("data", Json.toJson(pagedFlows));

					return resultNode;
				}
			});
			return result;
		}
		resultNode.put("count", 0);
		resultNode.put("page", page);
		resultNode.put("itemsPerPage", size);
		resultNode.put("totalPages", 0);
		resultNode.set("data", Json.toJson(""));
		return resultNode;
	}

	public static ObjectNode searchMetrics(JsonNode searchOpt, int page, int size)
	{
		ObjectNode resultNode = Json.newObject();
		int count = 0;
		List<String> dashboardInList = new ArrayList<String>();
		List<String> dashboardNotInList = new ArrayList<String>();
		List<String> groupInList = new ArrayList<String>();
		List<String> groupNotInList = new ArrayList<String>();
		List<String> categoryInList = new ArrayList<String>();
		List<String> categoryNotInList = new ArrayList<String>();
		List<String> metricInList = new ArrayList<String>();
		List<String> metricNotInList = new ArrayList<String>();

		if (searchOpt != null && (searchOpt.isContainerNode()))
		{
			if (searchOpt.has("dashboard")) {
				JsonNode dashboardNode = searchOpt.get("dashboard");
				if (dashboardNode != null && dashboardNode.isContainerNode())
				{
					if (dashboardNode.has("in"))
					{
						JsonNode dashboardInNode = dashboardNode.get("in");
						if (dashboardInNode != null)
						{
							String dashboardInStr = dashboardInNode.asText();
							if (StringUtils.isNotBlank(dashboardInStr))
							{
								String[] dashboardInArray = dashboardInStr.split(",");
								if (dashboardInArray != null)
								{
									for(String value : dashboardInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											dashboardInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (dashboardNode.has("not"))
					{
						JsonNode dashboardNotInNode = dashboardNode.get("not");
						if (dashboardNotInNode != null)
						{
							String dashboardNotInStr = dashboardNotInNode.asText();
							if (StringUtils.isNotBlank(dashboardNotInStr))
							{
								String[] dashboardNotInArray = dashboardNotInStr.split(",");
								if (dashboardNotInArray != null)
								{
									for(String value : dashboardNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											dashboardNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("group")) {
				JsonNode groupNode = searchOpt.get("group");
				if (groupNode != null && groupNode.isContainerNode())
				{
					if (groupNode.has("in"))
					{
						JsonNode groupInNode = groupNode.get("in");
						if (groupInNode != null)
						{
							String groupInStr = groupInNode.asText();
							if (StringUtils.isNotBlank(groupInStr))
							{
								String[] groupInArray = groupInStr.split(",");
								if (groupInArray != null)
								{
									for(String value : groupInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											groupInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (groupNode.has("not"))
					{
						JsonNode groupNotInNode = groupNode.get("not");
						if (groupNotInNode != null)
						{
							String groupNotInStr = groupNotInNode.asText();
							if (StringUtils.isNotBlank(groupNotInStr))
							{
								String[] groupNotInArray = groupNotInStr.split(",");
								if (groupNotInArray != null)
								{
									for(String value : groupNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											groupNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("cat")) {
				JsonNode categoryNode = searchOpt.get("cat");
				if (categoryNode != null && categoryNode.isContainerNode())
				{
					if (categoryNode.has("in"))
					{
						JsonNode categoryInNode = categoryNode.get("in");
						if (categoryInNode != null)
						{
							String categoryInStr = categoryInNode.asText();
							if (StringUtils.isNotBlank(categoryInStr))
							{
								String[] categoryInArray = categoryInStr.split(",");
								if (categoryInArray != null)
								{
									for(String value : categoryInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											categoryInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (categoryNode.has("not"))
					{
						JsonNode categoryNotInNode = categoryNode.get("not");
						if (categoryNotInNode != null)
						{
							String categoryNotInStr = categoryNotInNode.asText();
							if (StringUtils.isNotBlank(categoryNotInStr))
							{
								String[] categoryNotInArray = categoryNotInStr.split(",");
								if (categoryNotInArray != null)
								{
									for(String value : categoryNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											categoryNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			if (searchOpt.has("metric")) {
				JsonNode metricNode = searchOpt.get("metric");
				if (metricNode != null && metricNode.isContainerNode())
				{
					if (metricNode.has("in"))
					{
						JsonNode metricInNode = metricNode.get("in");
						if (metricInNode != null)
						{
							String metricInStr = metricInNode.asText();
							if (StringUtils.isNotBlank(metricInStr))
							{
								String[] metricInArray = metricInStr.split(",");
								if (metricInArray != null)
								{
									for(String value : metricInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											metricInList.add(value.trim());
										}
									}
								}
							}
						}
					}
					if (metricNode.has("not"))
					{
						JsonNode metricNotInNode = metricNode.get("not");
						if (metricNotInNode != null)
						{
							String metricNotInStr = metricNotInNode.asText();
							if (StringUtils.isNotBlank(metricNotInStr))
							{
								String[] metricNotInArray = metricNotInStr.split(",");
								if (metricNotInArray != null)
								{
									for(String value : metricNotInArray)
									{
										if (StringUtils.isNotBlank(value))
										{
											metricNotInList.add(value.trim());
										}
									}
								}
							}
						}
					}
				}
			}

			boolean needAndKeyword = false;

			final List<Metric> pagedMetrics = new ArrayList<Metric>();
			final JdbcTemplate jdbcTemplate = getJdbcTemplate();
			javax.sql.DataSource ds = jdbcTemplate.getDataSource();
			DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

			TransactionTemplate txTemplate = new TransactionTemplate(tm);

			ObjectNode result;
			String query = ADV_SEARCH_METRIC;

			if (dashboardInList.size() > 0 || dashboardNotInList.size() > 0)
			{
				boolean dashboardNeedAndKeyword = false;
				if (dashboardInList.size() > 0)
				{
					int indexForDashboardInList = 0;
					for (String dashboard : dashboardInList)
					{
						if (indexForDashboardInList == 0)
						{
							query += "WHERE dashboard_name in ('" + dashboard + "'";
						}
						else
						{
							query += ", '" + dashboard + "'";
						}
						indexForDashboardInList++;
					}
					query += ") ";
					dashboardNeedAndKeyword = true;
				}
				if (dashboardNotInList.size() > 0)
				{
					if (dashboardNeedAndKeyword)
					{
						query += " AND ";
					}
					else
					{
						query += " WHERE ";
					}
					int indexForDashboardNotInList = 0;
					for (String dashboard : dashboardNotInList)
					{
						if (indexForDashboardNotInList == 0)
						{
							query += "dashboard_name not in ('" + dashboard + "'";
						}
						else
						{
							query += ", '" + dashboard + "'";
						}
						indexForDashboardNotInList++;
					}
					query += ") ";
				}
				needAndKeyword = true;
			}

			if (groupInList.size() > 0 || groupNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " AND ";
				}
				else
				{
					query += " WHERE ";
				}
				query += "( ";
				boolean groupNeedAndKeyword = false;
				if (groupInList.size() > 0)
				{
					query += "( ";
					int indexForGroupInList = 0;
					for (String group : groupInList)
					{
						if (indexForGroupInList == 0)
						{
							query += "metric_group LIKE '%" + group + "%'";
						}
						else
						{
							query += " or metric_group LIKE '%" + group + "%'";
						}
						indexForGroupInList++;
					}
					query += ") ";
					groupNeedAndKeyword = true;
				}
				if (groupNotInList.size() > 0)
				{
					if (groupNeedAndKeyword)
					{
						query += " AND ";
					}
					query += "( ";
					int indexForGroupNotInList = 0;
					for (String group : groupNotInList)
					{
						if (indexForGroupNotInList == 0)
						{
							query += "metric_group NOT LIKE '%" + group + "%'";
						}
						else
						{
							query += " and metric_group NOT LIKE '%" + group + "%'";
						}
						indexForGroupNotInList++;
					}
					query += ") ";
				}
				query += ") ";
				needAndKeyword = true;
			}

			if (categoryInList.size() > 0 || categoryNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " AND ";
				}
				else
				{
					query += " WHERE ";
				}
				query += "( ";
				boolean categoryNeedAndKeyword = false;
				if (categoryInList.size() > 0)
				{
					int indexForCategoryInList = 0;
					query += "( ";
					for (String category : categoryInList)
					{
						if (indexForCategoryInList == 0)
						{
							query += "metric_category LIKE '%" + category + "%'";
						}
						else
						{
							query += " or metric_category LIKE '%" + category + "%'";
						}
						indexForCategoryInList++;
					}
					query += ") ";
					categoryNeedAndKeyword = true;
				}
				if (categoryNotInList.size() > 0)
				{
					if (categoryNeedAndKeyword)
					{
						query += " AND ";
					}
					query += "( ";
					int indexForCategoryNotInList = 0;
					for (String category : categoryNotInList)
					{
						if (indexForCategoryNotInList == 0)
						{
							query += "metric_category NOT LIKE '%" + category + "%'";
						}
						else
						{
							query += " and metric_category NOT LIKE '%" + category + "%'";
						}
						indexForCategoryNotInList++;
					}
					query += ") ";
				}
				query += ") ";
				needAndKeyword = true;
			}

			if (metricInList.size() > 0 || metricNotInList.size() > 0)
			{
				if (needAndKeyword)
				{
					query += " AND ";
				}
				else
				{
					query += " WHERE ";
				}
				query += "( ";
				boolean metricNeedAndKeyword = false;
				if (metricInList.size() > 0)
				{
					int indexForMetricInList = 0;
					query += " ( ";
					for (String metric : metricInList)
					{
						if (indexForMetricInList == 0)
						{
							query += "metric_name LIKE '%" + metric + "%'";
						}
						else
						{
							query += " or metric_name LIKE '%" + metric + "%'";
						}
						indexForMetricInList++;
					}
					query += ") ";
					metricNeedAndKeyword = true;
				}
				if (metricNotInList.size() > 0)
				{
					if (metricNeedAndKeyword)
					{
						query += " AND ";
					}
					query += "( ";
					int indexForMetricNotInList = 0;
					for (String metric : metricNotInList)
					{
						if (indexForMetricNotInList == 0)
						{
							query += "metric_name NOT LIKE '%" + metric + "%'";
						}
						else
						{
							query += " and metric_name NOT LIKE '%" + metric + "%'";
						}
						indexForMetricNotInList++;
					}
					query += ") ";
				}
				query += " )";
			}

			query += " LIMIT " + (page-1)*size + ", " + size;
			final String queryString = query;

			result = txTemplate.execute(new TransactionCallback<ObjectNode>()
			{
				public ObjectNode doInTransaction(TransactionStatus status)
				{
					List<Metric> pagedMetrics = jdbcTemplate.query(queryString, new MetricRowMapper());

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
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
					resultNode.set("data", Json.toJson(pagedMetrics));

					return resultNode;
				}
			});
			return result;
		}
		resultNode.put("count", 0);
		resultNode.put("page", page);
		resultNode.put("itemsPerPage", size);
		resultNode.put("totalPages", 0);
		resultNode.set("data", Json.toJson(""));
		return resultNode;
	}

}
