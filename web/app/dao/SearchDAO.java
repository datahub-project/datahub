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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;
import models.*;

public class SearchDAO extends AbstractMySQLOpenSourceDAO
{
	public final static String SEARCH_DATASET_WITH_PAGINATION = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, `name`, `schema`, `source`, `urn`, FROM_UNIXTIME(source_modified_time) as modified, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 as rank " +
			"FROM (SELECT id, `name`, `schema`, `source`, `urn`, source_modified_time, " +
			"CASE WHEN match(`name`) against ('$keyword' IN BOOLEAN MODE) THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN match(`name`) against ('$keyword*' IN BOOLEAN MODE) THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN match(`name`) against ('*$keyword*' IN BOOLEAN MODE) THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN match(`urn`) against ('$keyword' IN BOOLEAN MODE) THEN 300 ELSE 0 END rank_04, " +
			"CASE WHEN match(`urn`) against ('$keyword*' IN BOOLEAN MODE) THEN 200 ELSE 0 END rank_05, " +
			"CASE WHEN match(`urn`) against ('*$keyword*' IN BOOLEAN MODE) THEN 100 ELSE 0 END rank_06, " +
			"CASE WHEN match(`schema`) against ('$keyword' IN BOOLEAN MODE) THEN 30 ELSE 0 END rank_07, " +
			"CASE WHEN match(`schema`) against ('$keyword*' IN BOOLEAN MODE) THEN 20 ELSE 0 END rank_08, " +
			"CASE WHEN match(`schema`) against ('*$keyword*' IN BOOLEAN MODE) THEN 10 ELSE 0 END rank_09, " +
			"CASE WHEN match(`schema`) against ('*v_$keyword*' IN BOOLEAN MODE) THEN 5 ELSE 0 END rank_10 " +
			"FROM dict_dataset WHERE MATCH(`name`, `schema`,  `properties`, `urn`)" +
			" AGAINST ('*$keyword* *v_$keyword*' IN BOOLEAN MODE) ) t " +
			"ORDER BY rank DESC, `name`, `urn` LIMIT ?, ?;";

	public final static String SEARCH_DATASET_BY_SOURCE_WITH_PAGINATION = "SELECt SQL_CALC_FOUND_ROWS " +
			"id, `name`, `schema`, `source`, `urn`, FROM_UNIXTIME(source_modified_time) as modified, " +
			"rank_01 + rank_02 + rank_03 + rank_04 + rank_05 + rank_06 + rank_07 + rank_08 + rank_09 as rank " +
			"FROM (SELECT id, `name`, `schema`, `source`, `urn`, source_modified_time, " +
			"CASE WHEN match(`name`) against ('$keyword' IN BOOLEAN MODE) THEN 3000 ELSE 0 END rank_01, " +
			"CASE WHEN match(`name`) against ('$keyword*' IN BOOLEAN MODE) THEN 2000 ELSE 0 END rank_02, " +
			"CASE WHEN match(`name`) against ('*$keyword*' IN BOOLEAN MODE) THEN 1000 ELSE 0 END rank_03, " +
			"CASE WHEN match(`urn`) against ('$keyword' IN BOOLEAN MODE) THEN 300 ELSE 0 END rank_04, " +
			"CASE WHEN match(`urn`) against ('$keyword*' IN BOOLEAN MODE) THEN 200 ELSE 0 END rank_05, " +
			"CASE WHEN match(`urn`) against ('*$keyword*' IN BOOLEAN MODE) THEN 100 ELSE 0 END rank_06, " +
			"CASE WHEN match(`schema`) against ('$keyword' IN BOOLEAN MODE) THEN 30 ELSE 0 END rank_07, " +
			"CASE WHEN match(`schema`) against ('$keyword*' IN BOOLEAN MODE) THEN 20 ELSE 0 END rank_08, " +
			"CASE WHEN match(`schema`) against ('*$keyword*' IN BOOLEAN MODE) THEN 10 ELSE 0 END rank_09, " +
			"CASE WHEN match(`schema`) against ('*v_$keyword*' IN BOOLEAN MODE) THEN 5 ELSE 0 END rank_10 " +
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

	public final static String GET_METRIC_AUTO_COMPLETE_LIST = "SELECT DISTINCT CASE " +
			"WHEN parent_path is null or parent_path = '' THEN field_name " +
			"ELSE CONCAT_WS('.', parent_path,  field_name) END as full_name FROM dict_field_detail";

	public final static String GET_DATASET_AUTO_COMPLETE_LIST = "SELECT DISTINCT name FROM dict_dataset";

	public final static String GET_FLOW_AUTO_COMPLETE_LIST = "SELECT DISTINCT flow_name FROM flow";

	public final static String GET_JOB_AUTO_COMPLETE_LIST = "SELECT DISTINCT job_name FROM flow_job";

	public static List<String> getAutoCompleteList()
	{
		//List<String> metricList = getJdbcTemplate().queryForList(GET_METRIC_AUTO_COMPLETE_LIST, String.class);
		List<String> flowList = getJdbcTemplate().queryForList(GET_FLOW_AUTO_COMPLETE_LIST, String.class);
		List<String> jobList = getJdbcTemplate().queryForList(GET_JOB_AUTO_COMPLETE_LIST, String.class);
		List<String> datasetList = getJdbcTemplate().queryForList(GET_DATASET_AUTO_COMPLETE_LIST, String.class);
		List<String> autoCompleteList =
				Stream.concat(datasetList.stream(),
						Stream.concat(flowList.stream(), jobList.stream())).collect(Collectors.toList());
		Collections.sort(autoCompleteList);

		return autoCompleteList;
	}

	public static ObjectNode getPagedDatasetByKeyword(String keyword, String source, int page, int size)
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
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedDatasets));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedMetricByKeyword(String keyword, int page, int size)
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
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedMetrics));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedFlowByKeyword(String keyword, int page, int size)
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
					flow.link = "#/flows/" + flow.appCode + "/" +
							flow.flowGroup + "/" + Long.toString(flow.flowId) + "/page/1";
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
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedFlows));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedJobByKeyword(String keyword, int page, int size)
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
					flowJob.link =  "#/flows/" + flowJob.appCode + "/" +
							flowJob.flowGroup + "/" + Long.toString(flowJob.flowId) + "/page/1";
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
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedFlowJobs));

				return resultNode;
			}
		});

		return result;
	}

	public static ObjectNode getPagedCommentsByKeyword(String keyword, int page, int size)
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
				Logger.error(query);
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
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("data", Json.toJson(pagedDatasets));

				return resultNode;
			}
		});

		return result;
	}

}
