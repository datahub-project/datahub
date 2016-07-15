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
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;

public class FlowsDAO extends AbstractMySQLOpenSourceDAO
{
	private final static String GET_APP_ID  =
			"SELECT app_id FROM cfg_application WHERE LOWER(app_code) = ?";

	private final static String GET_PAGED_PROJECTS = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT IFNULL(f.flow_group, 'ROOT') as project_name, f.app_id, f.flow_group, a.app_code " +
			"FROM flow f JOIN cfg_application a ON f.app_id = a.app_id GROUP BY 1 limit ?, ?";

	private final static String GET_PAGED_PROJECTS_BY_APP_ID = "SELECT SQL_CALC_FOUND_ROWS " +
			"distinct IFNULL(f.flow_group, 'ROOT') as project_name, f.app_id, f.flow_group, a.app_code " +
			"FROM flow f JOIN cfg_application a ON f.app_id = a.app_id WHERE f.app_id = ? GROUP BY 1 limit ?, ?";

	private final static String GET_FLOW_COUNT_BY_APP_ID_AND_PROJECT_NAME = "SELECT count(*) " +
			"FROM flow WHERE app_id = ? and flow_group = ?";

	private final static String GET_FLOW_COUNT_WITHOUT_PROJECT_BY_APP_ID = "SELECT count(*) " +
			"FROM flow WHERE app_id = ? and flow_group is null";

	private final static String GET_PAGED_FLOWS = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT f.flow_id, f.flow_name, f.flow_path, f.flow_group, f.flow_level, f.app_id, ca.app_code, " +
			"FROM_UNIXTIME(f.created_time) as created_time, FROM_UNIXTIME(f.modified_time) as modified_time " +
			"FROM flow f JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE (f.is_active is null or f.is_active = 'Y') ORDER BY 2 LIMIT ?, ?";

	private final static String GET_PAGED_FLOWS_BY_APP_ID = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT f.flow_id, f.flow_name, f.flow_path, f.flow_group, f.flow_level, f.app_id, ca.app_code, " +
			"FROM_UNIXTIME(f.created_time) as created_time, FROM_UNIXTIME(f.modified_time) as modified_time " +
			"FROM flow f JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE f.app_id = ? and (f.is_active is null or f.is_active = 'Y') ORDER BY 2 LIMIT ?, ?";

	private final static String GET_PAGED_FLOWS_BY_APP_ID_AND_PROJECT_NAME = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT f.flow_id, f.flow_name, f.flow_path, f.flow_group, f.flow_level, f.app_id, ca.app_code, " +
            "FROM_UNIXTIME(f.created_time) as created_time, FROM_UNIXTIME(f.modified_time) as modified_time " +
			"FROM flow f JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE f.app_id = ? and f.flow_group = ? and (f.is_active is null or f.is_active = 'Y') " +
			"ORDER BY 2 LIMIT ?, ?";

	private final static String GET_PAGED_FLOWS_WITHOUT_PROJECT_BY_APP_ID = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT f.flow_id, f.flow_name, f.flow_path, f.flow_group, f.flow_level, f.app_id, ca.app_code, " +
            "FROM_UNIXTIME(f.created_time) as created_time, FROM_UNIXTIME(f.modified_time) as modified_time " +
			"FROM flow f JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE f.app_id = ? and f.flow_group is null and (f.is_active is null or f.is_active = 'Y') " +
			"ORDER BY 2 LIMIT ?, ?";

	private final static String GET_JOB_COUNT_BY_APP_ID_AND_FLOW_ID =
			"SELECT count(*) FROM flow_job WHERE app_id = ? and flow_id = ?";

	private final static String GET_PAGED_JOBS_BY_APP_ID_AND_FLOW_ID = "select SQL_CALC_FOUND_ROWS " +
			"j.job_id, MAX(j.last_source_version), j.job_name, j.job_path, j.job_type, j.ref_flow_id, " +
			"FROM_UNIXTIME(j.created_time) as created_time, " +
			"FROM_UNIXTIME(j.modified_time) as modified_time, f.flow_name, l.flow_group " +
			"FROM flow_job j JOIN flow f on j.app_id = f.app_id and j.flow_id = f.flow_id " +
			"LEFT JOIN flow l on j.app_id = l.app_id and j.ref_flow_id = l.flow_id " +
			"WHERE j.app_id = ? and j.flow_id = ? GROUP BY j.job_id, j.job_name, " +
			"j.job_path, j.job_type, j.ref_flow_id, " +
			"f.flow_name ORDER BY j.job_id LIMIT ?, ?";

	private final static String GET_FLOW_TREE_APPLICATON_NODES = "SELECT DISTINCT ca.app_code " +
			"From flow f JOIN cfg_application ca ON f.app_id = ca.app_id ORDER by app_code";

	private final static String GET_FLOW_TREE_PROJECT_NODES = "SELECT DISTINCT IFNULL(f.flow_group, 'ROOT') " +
			"FROM flow f JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE (is_active is null or is_active = 'Y') and ca.app_code = ? ORDER BY flow_group";

	private final static String GET_FLOW_TREE_FLOW_NODES = "SELECT DISTINCT f.flow_id, f.flow_name FROM flow f " +
			"JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE (f.is_active is null or f.is_active = 'Y') and ca.app_code = ? " +
			"and f.flow_group = ? ORDER BY f.flow_name";

	private final static String GET_FLOW_TREE_FLOW_NODES_WITHOUT_PROJECT = "SELECT DISTINCT " +
			"f.flow_id, f.flow_name FROM flow f " +
			"JOIN cfg_application ca ON f.app_id = ca.app_id " +
			"WHERE (f.is_active is null or f.is_active = 'Y') and ca.app_code = ? " +
			"and f.flow_group is null ORDER BY f.flow_name";

	private final static String GET_FLOW_LIST_VIEW_CLUSTER_NODES = "SELECT DISTINCT f.app_id, a.app_code " +
			"FROM flow f JOIN cfg_application a ON f.app_id = a.app_id ORDER BY a.app_code";

	private final static String GET_FLOW_LIST_VIEW_PROJECT_NODES = "SELECT DISTINCT f.flow_group, " +
			"a.app_id, a.app_code FROM flow f JOIN cfg_application a ON f.app_id = a.app_id " +
			"WHERE a.app_code = ? ORDER BY f.flow_group";

	private final static String GET_FLOW_LIST_VIEW_FLOW_NODES = "SELECT DISTINCT f.flow_name, f.flow_group, " +
			"f.flow_id, a.app_id, a.app_code FROM flow f JOIN cfg_application a ON f.app_id = a.app_id " +
			"WHERE a.app_code = ? and f.flow_group = ? order by f.flow_name";

	public static Integer getApplicationIDByName(String applicationName)
	{
		Integer applicationId = 0;
		try {
			applicationId = getJdbcTemplate().queryForObject(
					GET_APP_ID,
					new Object[]{applicationName.replace(".", " ")},
					Integer.class);
		} catch (EmptyResultDataAccessException e) {
			applicationId = 0;
			Logger.error("Get application id failed, application name = " + applicationName);
			Logger.error("Exception = " + e.getMessage());
		}

		return applicationId;
	}

	public static JsonNode getFlowApplicationNodes()
	{
		List<String> appList = getJdbcTemplate().queryForList(GET_FLOW_TREE_APPLICATON_NODES, String.class);
		List<TreeNode> nodes = new ArrayList<TreeNode>();
		if (appList != null && appList.size() > 0)
		{
			for (String app : appList)
			{
				TreeNode node = new TreeNode();
				node.folder = true;
				node.lazy = true;
				node.title = app;
				node.level = 1;
				nodes.add(node);
			}
		}
		return Json.toJson(nodes);
	}

	public static JsonNode getFlowProjectNodes(String app)
	{
		List<String> projectList = getJdbcTemplate().queryForList(GET_FLOW_TREE_PROJECT_NODES, String.class, app);
		List<TreeNode> nodes = new ArrayList<TreeNode>();
		if (projectList != null && projectList.size() > 0)
		{
			for (String project : projectList)
			{
				TreeNode node = new TreeNode();
				node.folder = true;
				node.lazy = true;
				node.title = project;
				node.parent = app;
				node.level = 2;
				nodes.add(node);
			}
		}
		return Json.toJson(nodes);
	}

	public static JsonNode getFlowNodes(String app, String project)
	{
		List<Map<String, Object>> rows = null;

		if (StringUtils.isBlank(project) || project.equalsIgnoreCase("root"))
		{
			rows = getJdbcTemplate().queryForList(GET_FLOW_TREE_FLOW_NODES_WITHOUT_PROJECT, app);
		}
		else
		{
			rows = getJdbcTemplate().queryForList(GET_FLOW_TREE_FLOW_NODES, app, project);
		}

		List<TreeNode> nodes = new ArrayList<TreeNode>();
		if (rows != null)
		{
			for (Map row : rows)
			{
				TreeNode node = new TreeNode();
				node.folder = false;
				node.lazy = false;
				node.title = (String)row.get("flow_name");
				node.parent = project;
				node.id = (Long)row.get("flow_id");
				node.level = 3;
				nodes.add(node);
			}
		}
		return Json.toJson(nodes);
	}

	public static ObjectNode getPagedProjects(int page, int size)
	{
		ObjectNode result;

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				ObjectNode resultNode = Json.newObject();
				long count = 0;
				List<Flow> pagedFlows = new ArrayList<Flow>();
				List<Map<String, Object>> rows = null;
				rows = getJdbcTemplate().queryForList(
						GET_PAGED_FLOWS,
						(page - 1) * size,
						size);

				try {
					count = getJdbcTemplate().queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}
				for (Map row : rows) {
					Flow flow = new Flow();
					flow.id = (Long)row.get("flow_id");
					flow.level = (Integer)row.get("flow_level");
					flow.appId = (Integer)row.get("app_id");
					flow.group = (String)row.get("flow_group");
					flow.name = (String)row.get("flow_name");
					flow.path = (String)row.get("flow_path");
					flow.appCode = (String)row.get("app_code");
					if (StringUtils.isNotBlank(flow.path))
					{
						int index = flow.path.indexOf(":");
						if (index != -1)
						{
							flow.path = flow.path.substring(0, index);
						}
					}
					Object created = row.get("created_time");
					if (created != null)
					{
						flow.created = created.toString();
					}
					Object modified = row.get("modified_time");
					if (modified != null)
					{
						flow.modified = row.get("modified_time").toString();
					}

					int jobCount = 0;

					if (flow.id != null && flow.id != 0)
					{
						try {
							jobCount = getJdbcTemplate().queryForObject(
									GET_JOB_COUNT_BY_APP_ID_AND_FLOW_ID,
									new Object[] {flow.appId, flow.id},
									Integer.class);
							flow.jobCount = jobCount;
						}
						catch(EmptyResultDataAccessException e)
						{
							Logger.error("Exception = " + e.getMessage());
						}
					}
					pagedFlows.add(flow);
				}
				resultNode.set("flows", Json.toJson(pagedFlows));
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));

				return resultNode;
			}
		});
		return result;
	}

	public static ObjectNode getPagedProjectsByApplication(String applicationName, int page, int size)
	{
		String application = applicationName.replace(".", " ");
		ObjectNode result = Json.newObject();

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		Integer appID = getApplicationIDByName(applicationName);
		if (appID != 0) {

			final int applicationID = appID;

			result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
				public ObjectNode doInTransaction(TransactionStatus status)
				{
					ObjectNode resultNode = Json.newObject();
					long count = 0;
					List<Flow> pagedFlows = new ArrayList<Flow>();
					List<Map<String, Object>> rows = null;
					rows = getJdbcTemplate().queryForList(
							GET_PAGED_FLOWS_BY_APP_ID,
							applicationID,
							(page - 1) * size,
							size);

					try {
						count = getJdbcTemplate().queryForObject(
								"SELECT FOUND_ROWS()",
								Long.class);
					}
					catch(EmptyResultDataAccessException e)
					{
						Logger.error("Exception = " + e.getMessage());
					}
					for (Map row : rows) {
						Flow flow = new Flow();
						flow.id = (Long)row.get("flow_id");
						flow.level = (Integer)row.get("flow_level");
						flow.appId = (Integer)row.get("app_id");
						flow.group = (String)row.get("flow_group");
						flow.name = (String)row.get("flow_name");
						flow.path = (String)row.get("flow_path");
						flow.appCode = (String)row.get("app_code");
						if (StringUtils.isNotBlank(flow.path))
						{
							int index = flow.path.indexOf(":");
							if (index != -1)
							{
								flow.path = flow.path.substring(0, index);
							}
						}
						Object created = row.get("created_time");
						if (created != null)
						{
							flow.created = created.toString();
						}
						Object modified = row.get("modified_time");
						if (modified != null)
						{
							flow.modified = row.get("modified_time").toString();
						}

						int jobCount = 0;

						if (flow.id != null && flow.id != 0)
						{
							try {
								jobCount = getJdbcTemplate().queryForObject(
										GET_JOB_COUNT_BY_APP_ID_AND_FLOW_ID,
										new Object[] {flow.appId, flow.id},
										Integer.class);
								flow.jobCount = jobCount;
							}
							catch(EmptyResultDataAccessException e)
							{
								Logger.error("Exception = " + e.getMessage());
							}
						}
						pagedFlows.add(flow);
					}
					resultNode.set("flows", Json.toJson(pagedFlows));
					resultNode.put("count", count);
					resultNode.put("page", page);
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));

					return resultNode;
				}
			});
			return result;
		}

		result = Json.newObject();
		result.put("count", 0);
		result.put("page", page);
		result.put("itemsPerPage", size);
		result.put("totalPages", 0);
		result.set("flows", Json.toJson(""));
		return result;
	}

	public static ObjectNode getPagedFlowsByProject(String applicationName, String project, int page, int size)
	{
		ObjectNode result;

		if (StringUtils.isBlank(applicationName) || StringUtils.isBlank(project))
		{
			result = Json.newObject();
			result.put("count", 0);
			result.put("page", page);
			result.put("itemsPerPage", size);
			result.put("totalPages", 0);
			result.set("flows", Json.toJson(""));
			return result;
		}

		String application = applicationName.replace(".", " ");

		Integer appID = getApplicationIDByName(applicationName);
		if (appID != 0)
		{
			javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
			DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
			TransactionTemplate txTemplate = new TransactionTemplate(tm);
			final int applicationID = appID;
			result = txTemplate.execute(new TransactionCallback<ObjectNode>()
			{
				public ObjectNode doInTransaction(TransactionStatus status)
				{
					ObjectNode resultNode = Json.newObject();
					long count = 0;
					List<Flow> pagedFlows = new ArrayList<Flow>();
					List<Map<String, Object>> rows = null;
					if (StringUtils.isNotBlank(project) && (!project.equalsIgnoreCase("root")))
					{
						rows = getJdbcTemplate().queryForList(
								GET_PAGED_FLOWS_BY_APP_ID_AND_PROJECT_NAME,
								applicationID,
								project,
								(page - 1) * size,
								size);
					}
					else
					{
						rows = getJdbcTemplate().queryForList(
								GET_PAGED_FLOWS_WITHOUT_PROJECT_BY_APP_ID,
								applicationID,
								(page - 1) * size,
								size);
					}

                    try {
						count = getJdbcTemplate().queryForObject(
								"SELECT FOUND_ROWS()",
								Long.class);
					}
					catch(EmptyResultDataAccessException e)
					{
						Logger.error("Exception = " + e.getMessage());
					}
					for (Map row : rows) {
                    	Flow flow = new Flow();
						flow.id = (Long)row.get("flow_id");
						flow.level = (Integer)row.get("flow_level");
						flow.name = (String)row.get("flow_name");
						flow.path = (String)row.get("flow_path");
						flow.appCode = (String)row.get("app_code");
						flow.group = project;
						if (StringUtils.isNotBlank(flow.path))
						{
							int index = flow.path.indexOf(":");
							if (index != -1)
							{
								flow.path = flow.path.substring(0, index);
							}
						}
						Object created = row.get("created_time");
						if (created != null)
						{
							flow.created = created.toString();
						}
						Object modified = row.get("modified_time");
						if (modified != null)
						{
							flow.modified = row.get("modified_time").toString();
						}

						int jobCount = 0;

						if (flow.id != null && flow.id != 0)
						{
							try {
								jobCount = getJdbcTemplate().queryForObject(
										GET_JOB_COUNT_BY_APP_ID_AND_FLOW_ID,
										new Object[] {appID, flow.id},
										Integer.class);
								flow.jobCount = jobCount;
							}
							catch(EmptyResultDataAccessException e)
							{
								Logger.error("Exception = " + e.getMessage());
							}
                    	}
						pagedFlows.add(flow);
					}
					resultNode.set("flows", Json.toJson(pagedFlows));
					resultNode.put("count", count);
					resultNode.put("page", page);
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));

					return resultNode;
				}
			});
			return result;
		}

		result = Json.newObject();
		result.put("count", 0);
		result.put("page", page);
		result.put("itemsPerPage", size);
		result.put("totalPages", 0);
		result.set("flows", Json.toJson(""));
		return result;
	}

	public static ObjectNode getPagedJobsByFlow(
			String applicationName,
			Long flowId,
			int page,
			int size)
	{
		ObjectNode result;
		List<Job> pagedJobs = new ArrayList<Job>();

		if (StringUtils.isBlank(applicationName) || (flowId <= 0))
		{
			result = Json.newObject();
			result.put("count", 0);
			result.put("page", page);
			result.put("itemsPerPage", size);
			result.put("totalPages", 0);
			result.set("jobs", Json.toJson(""));
			return result;
		}

		String application = applicationName.replace(".", " ");

		Integer appID = getApplicationIDByName(application);
		if (appID != 0)
		{
			javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
			DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
			TransactionTemplate txTemplate = new TransactionTemplate(tm);
			final long azkabanFlowId = flowId;
			result = txTemplate.execute(new TransactionCallback<ObjectNode>()
			{
				public ObjectNode doInTransaction(TransactionStatus status)
				{
					List<Map<String, Object>> rows = null;
					rows = getJdbcTemplate().queryForList(
							GET_PAGED_JOBS_BY_APP_ID_AND_FLOW_ID,
							appID,
							azkabanFlowId,
							(page - 1) * size,
							size);
					long count = 0;
					String flowName = "";
					try {
						count = getJdbcTemplate().queryForObject(
								"SELECT FOUND_ROWS()",
								Long.class);
					}
					catch(EmptyResultDataAccessException e)
					{
						Logger.error("Exception = " + e.getMessage());
					}
					for (Map row : rows)
					{
            			Job job = new Job();
						job.id = (Long)row.get("job_id");
						job.name = (String)row.get("job_name");
						job.path = (String)row.get("job_path");
						job.path = (String)row.get("job_path");
						job.refFlowGroup = (String)row.get("flow_group");
						if (StringUtils.isNotBlank(job.path))
						{
							int index = job.path.indexOf("/");
							if (index != -1)
							{
								job.path = job.path.substring(0, index);
							}
						}
						job.type = (String)row.get("job_type");
						Object created = row.get("created_time");
						job.refFlowId = (Long)row.get("ref_flow_id");
						if (created != null)
						{
							job.created = created.toString();
						}
						Object modified = row.get("modified_time");
						if (modified != null)
						{
							job.modified = modified.toString();
						}

						if (StringUtils.isBlank(flowName))
						{
							flowName = (String)row.get("flow_name");
						}
						pagedJobs.add(job);
					}
					ObjectNode resultNode = Json.newObject();
					resultNode.put("count", count);
					resultNode.put("flow", flowName);
					resultNode.put("page", page);
					resultNode.put("itemsPerPage", size);
					resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
					resultNode.set("jobs", Json.toJson(pagedJobs));
					return resultNode;
				}
			});
			return result;
		}

		result = Json.newObject();
		result.put("count", 0);
		result.put("page", page);
		result.put("itemsPerPage", size);
		result.put("totalPages", 0);
		result.set("jobs", Json.toJson(""));
		return result;
	}

	public static List<FlowListViewNode> getFlowListViewClusters()
	{
		List<FlowListViewNode> nodes = new ArrayList<FlowListViewNode>();
		List<Map<String, Object>> rows = null;

		rows = getJdbcTemplate().queryForList(
				GET_FLOW_LIST_VIEW_CLUSTER_NODES);
		for (Map row : rows) {

			FlowListViewNode node = new FlowListViewNode();
			node.application = (String) row.get(FlowRowMapper.APP_CODE_COLUMN);
			node.nodeName = node.application;
			node.nodeUrl = "#/flows/name/" + node.nodeName + "/page/1?urn=" + node.nodeName;
			nodes.add(node);
		}
		return nodes;
	}

	public static List<FlowListViewNode> getFlowListViewProjects(String application)
	{
		List<FlowListViewNode> nodes = new ArrayList<FlowListViewNode>();
		List<Map<String, Object>> rows = null;

		rows = getJdbcTemplate().queryForList(
				GET_FLOW_LIST_VIEW_PROJECT_NODES,
				application);
		for (Map row : rows) {

			FlowListViewNode node = new FlowListViewNode();
			node.application = (String) row.get(FlowRowMapper.APP_CODE_COLUMN);
			node.project = (String) row.get(FlowRowMapper.FLOW_GROUP_COLUMN);
			node.nodeName = node.project;
			node.nodeUrl = "#/flows/name/" + node.project + "/page/1?urn=" + node.application + "/" + node.project;
			nodes.add(node);
		}
		return nodes;
	}

	public static List<FlowListViewNode> getFlowListViewFlows(String application, String project)
	{
		List<FlowListViewNode> nodes = new ArrayList<FlowListViewNode>();
		List<Map<String, Object>> rows = null;

		rows = getJdbcTemplate().queryForList(
				GET_FLOW_LIST_VIEW_FLOW_NODES,
				application,
				project);
		for (Map row : rows) {

			FlowListViewNode node = new FlowListViewNode();
			node.application = (String) row.get(FlowRowMapper.APP_CODE_COLUMN);
			node.project = (String) row.get(FlowRowMapper.FLOW_GROUP_COLUMN);
			node.flow = (String) row.get(FlowRowMapper.FLOW_NAME_COLUMN);
			node.nodeName = node.flow;
			node.flowId = (Long)row.get(FlowRowMapper.FLOW_ID_COLUMN);
			node.nodeUrl = "#/flows/name/" + node.application + "/"
					+ Long.toString(node.flowId) + "/page/1?urn=" + node.project;
			nodes.add(node);
		}
		return nodes;
	}
}
