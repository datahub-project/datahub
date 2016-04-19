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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import play.Logger;
import play.Play;
import play.libs.Json;
import utils.Lineage;

public class LineageDAO extends AbstractMySQLOpenSourceDAO
{
	private final static String GET_APPLICATION_ID = "SELECT DISTINCT app_id FROM " +
			"job_execution_data_lineage WHERE abstracted_object_name = ?";

	private final static String GET_FLOW_NAME = "SELECT flow_name FROM " +
			"flow WHERE app_id = ? and flow_id = ?";

	private final static String GET_JOB = "SELECT ca.app_id, ca.app_code as cluster, " +
			"je.flow_id, je.job_id, jedl.job_name, " +
			"fj.job_path, fj.job_type, jedl.flow_path, jedl.storage_type, jedl.source_target_type, " +
			"jedl.operation, jedl.source_srl_no, jedl.srl_no, " +
			"max(jedl.job_exec_id) as job_exec_id, jedl.job_start_unixtime, jedl.job_finished_unixtime," +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"JOIN job_execution je on jedl.app_id = je.app_id " +
			"and jedl.flow_exec_id = je.flow_exec_id and jedl.job_exec_id = je.job_exec_id " +
			"LEFT JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and je.job_id = fj.job_id " +
			"WHERE abstracted_object_name in ( :names ) and " +
			"jedl.flow_path not REGEXP '^(rent-metrics:|tracking-investigation:)' and " +
			"COALESCE(jedl.source_srl_no, jedl.srl_no) = jedl.srl_no and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL (:days) DAY " +
			"GROUP BY ca.app_id, je.job_id, je.flow_id, jedl.source_target_type, jedl.storage_type " +
			"ORDER BY jedl.source_target_type DESC, jedl.job_finished_unixtime";

	private final static String GET_JOB_WITH_SOURCE = "SELECT ca.app_id, ca.app_code as cluster, " +
			"je.flow_id, je.job_id, jedl.job_name, " +
			"fj.job_path, fj.job_type, jedl.flow_path, jedl.storage_type, jedl.source_target_type, " +
			"jedl.operation, jedl.source_srl_no, jedl.srl_no, " +
			"max(jedl.job_exec_id) as job_exec_id, jedl.job_start_unixtime, jedl.job_finished_unixtime," +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"JOIN job_execution je on jedl.app_id = je.app_id " +
			"and jedl.flow_exec_id = je.flow_exec_id and jedl.job_exec_id = je.job_exec_id " +
			"LEFT JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and je.job_id = fj.job_id " +
			"WHERE abstracted_object_name in ( :names ) and jedl.source_target_type != (:type) and " +
			"jedl.flow_path not REGEXP '^(rent-metrics:|tracking-investigation:)' and " +
			"COALESCE(jedl.source_srl_no, jedl.srl_no) = jedl.srl_no and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL (:days) DAY " +
			"GROUP BY ca.app_id, je.job_id, je.flow_id, jedl.source_target_type, jedl.storage_type " +
			"ORDER BY jedl.source_target_type DESC, jedl.job_finished_unixtime";

	private final static String GET_DATA = "SELECT storage_type, operation, " +
			"abstracted_object_name, source_target_type, job_start_unixtime, job_finished_unixtime " +
			"FROM job_execution_data_lineage WHERE app_id = ? and job_exec_id = ? and " +
			"flow_path not REGEXP '^(rent-metrics:|tracking-investigation:)' and " +
			"COALESCE(source_srl_no, srl_no) = srl_no ORDER BY source_target_type DESC";

	private final static String GET_DATA_WITH_SOURCE = "SELECT storage_type, operation, " +
			"abstracted_object_name, source_target_type, job_start_unixtime, job_finished_unixtime " +
			"FROM job_execution_data_lineage WHERE app_id = ? and job_exec_id = ? and source_target_type = ? and " +
			"flow_path not REGEXP '^(rent-metrics:|tracking-investigation:)' and " +
			"COALESCE(source_srl_no, srl_no) = srl_no ORDER BY source_target_type DESC";


	private final static String GET_APP_ID  = "SELECT app_id FROM cfg_application WHERE LOWER(app_code) = ?";


	private final static String GET_FLOW_JOB = "SELECT ca.app_id, ca.app_code, je.flow_id, je.job_id, " +
			"jedl.job_name, fj.job_path, fj.job_type, jedl.flow_path, jedl.storage_type, " +
			"jedl.source_target_type, jedl.operation, jedl.job_exec_id, fj.pre_jobs, fj.post_jobs, " +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time " +
			"FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"JOIN job_execution je on jedl.app_id = je.app_id and " +
			"jedl.flow_exec_id = je.flow_exec_id and jedl.job_exec_id = je.job_exec_id " +
			"JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and je.job_id = fj.job_id " +
			"WHERE jedl.app_id = ? and jedl.flow_exec_id = ? and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL ? DAY";

	private final static String GET_LATEST_FLOW_EXEC_ID = "SELECT max(flow_exec_id) FROM " +
			"flow_execution where app_id = ? and flow_id = ?";

	private final static String GET_FLOW_DATA_LINEAGE = "SELECT ca.app_code, jedl.job_exec_id, jedl.job_name, " +
			"jedl.storage_type, jedl.abstracted_object_name, jedl.source_target_type, jedl.record_count, " +
			"jedl.app_id, jedl.partition_type, jedl.operation, jedl.partition_start, " +
			"jedl.partition_end, jedl.full_object_name, " +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"WHERE jedl.app_id = ? and jedl.flow_exec_id = ? ORDER BY jedl.partition_end DESC";

	private final static String GET_ONE_LEVEL_IMPACT_DATABASES = "SELECT DISTINCT j.storage_type, " +
			"j.abstracted_object_name, d.id FROM job_execution_data_lineage j " +
			"LEFT JOIN dict_dataset d ON d.urn = concat(j.storage_type, '://', j.abstracted_object_name) " +
			"WHERE (app_id, job_exec_id) in ( " +
			"SELECT app_id, job_exec_id FROM job_execution_data_lineage " +
			"WHERE abstracted_object_name in (:pathlist) and source_target_type = 'source' and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL 60 DAY ) and " +
			"abstracted_object_name not like '/tmp/%' and abstracted_object_name not like '%tmp' " +
			"and source_target_type = 'target' and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL 60 DAY";

	private final static String GET_MAPPED_OBJECT_NAME = "SELECT mapped_object_name " +
			"FROM cfg_object_name_map WHERE object_name = ?";

	private final static String GET_OBJECT_NAME_BY_MAPPED_NAME = "SELECT object_name " +
			"FROM cfg_object_name_map WHERE mapped_object_name = ?";


	public static JsonNode getObjectAdjacnet(String urn, int upLevel, int downLevel, int lookBackTime)
	{
		ObjectNode resultNode = Json.newObject();
		LineagePathInfo pathInfo = utils.Lineage.convertFromURN(urn);
		List<LineageNode> nodes = new ArrayList<LineageNode>();
		List<LineageEdge> edges = new ArrayList<LineageEdge>();
		Map<Long, Integer> addedJobNodes = new HashMap<Long, Integer>();
		Map<String, LineageNode> addedSourceDataNode = new HashMap<String, LineageNode>();
		Map<String, List<LineageNode>> addedTargetDataNodes = new HashMap<String, List<LineageNode>>();
		String message = null;
		getObjectAdjacentNode(
				pathInfo,
				upLevel,
				downLevel,
				null,
				nodes,
				edges,
				addedSourceDataNode,
				addedTargetDataNodes,
				addedJobNodes,
				lookBackTime);
		if (nodes.size() > 0)
		{
			message = "Found lineage information";
		}
		else
		{
			message = "No lineage information found for this dataset";
		}
		resultNode.set("nodes", Json.toJson(nodes));
		resultNode.set("links", Json.toJson(edges));
		resultNode.put("urn", urn);
		resultNode.put("message", message);
		return resultNode;
	}

	public static List<String> getLiasDatasetNames(String abstractedObjectName)
	{
		if (StringUtils.isBlank(abstractedObjectName))
			return null;
		List<String> totalNames = new ArrayList<String>();
		totalNames.add(abstractedObjectName);
		List<String> mappedNames;
		mappedNames = getJdbcTemplate().queryForList(GET_MAPPED_OBJECT_NAME, String.class, abstractedObjectName);
		if (mappedNames != null)
		{
			totalNames.addAll(mappedNames);
			for (String name : mappedNames)
			{
				List<String> objNames = getJdbcTemplate().queryForList(
						GET_OBJECT_NAME_BY_MAPPED_NAME, String.class, name);
				{
					if (objNames != null)
					{
						totalNames.addAll(objNames);
					}
				}

			}
		}
		List<String> results = new ArrayList<String>();
		Set<String> sets = new HashSet<String>();
		sets.addAll(totalNames);
		results.addAll(sets);
		return results;
	}

	public static void searchInAzkaban(
			LineagePathInfo pathInfo,
			int upLevel,
			int downLevel,
			LineageNode currentNode,
			List<LineageNode> nodes,
			List<LineageEdge> edges,
			Map<String, LineageNode>addedSourceNode,
			Map<String, List<LineageNode>>addedTargetNodes,
			Map<Long, Integer> addedJobNodes,
			int lookBackTime)
	{
		if (upLevel < 1 && downLevel < 1)
		{
			return;
		}
		if (currentNode != null)
		{
			if ( StringUtils.isBlank(currentNode.source_target_type))
			{
				Logger.error("Source target type is not available");
				Logger.error(currentNode.abstracted_path);
				return;
			}
			else if (currentNode.source_target_type.equalsIgnoreCase("target") && downLevel <= 0)
			{
				Logger.warn("Dataset " + currentNode.abstracted_path + " downLevel = " + Integer.toString(downLevel));
				return;
			}
			else if (currentNode.source_target_type.equalsIgnoreCase("source") && upLevel <= 0)
			{
				Logger.warn("Dataset " + currentNode.abstracted_path + " upLevel = " + Integer.toString(upLevel));
				return;
			}
		}
		List<String> nameList = getLiasDatasetNames(pathInfo.filePath);
		List<Map<String, Object>> rows = null;
		MapSqlParameterSource parameters = new MapSqlParameterSource();
		parameters.addValue("names", nameList);
		NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
				NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
		parameters.addValue("days", lookBackTime);

		if (currentNode != null)
		{
			parameters.addValue("type", currentNode.source_target_type);
			rows = namedParameterJdbcTemplate.queryForList(
					GET_JOB_WITH_SOURCE,
					parameters);
			if (rows != null && rows.size() > 0)
			{
				if (upLevel >= 1)
				{
					List<LineageNode> addedTargets = addedTargetNodes.get(currentNode.abstracted_path);
					if (addedTargets == null)
					{
						addedTargets = new ArrayList<LineageNode>();
					}
					addedTargets.add(currentNode);
					addedTargetNodes.put(currentNode.abstracted_path, addedTargets);
					addedSourceNode.remove(currentNode.abstracted_path);
				}
			}
		} else {
			rows = namedParameterJdbcTemplate.queryForList(
					GET_JOB,
					parameters);
		}

		if (rows != null) {
			for (Map row : rows) {
				LineageNode node = new LineageNode();
				Object jobExecIdObject = row.get("job_exec_id");
				if (jobExecIdObject == null) {
					continue;
				}
				Long jobExecId = ((BigInteger) jobExecIdObject).longValue();
				Integer jobNodeId = addedJobNodes.get(jobExecId);
				if (jobNodeId != null) {
					continue;
				}
				node._sort_list = new ArrayList<String>();
				node.node_type = "script";
				node.job_type = (String) row.get("job_type");
				node.cluster = (String) row.get("cluster");
				node.job_path = (String) row.get("job_path");
				node.job_name = (String) row.get("job_name");
				node.job_start_time = row.get("start_time").toString();
				node.job_start_unix_time = (Long) row.get("job_start_unixtime");
				node.job_end_time = row.get("end_time").toString();
				node.job_end_unix_time = (Long) row.get("job_finished_unixtime");
				node.source_target_type = (String) row.get("source_target_type");
				node._sort_list.add("cluster");
				node._sort_list.add("job_path");
				node._sort_list.add("job_name");
				node._sort_list.add("job_type");
				node._sort_list.add("job_start_time");
				node._sort_list.add("job_end_time");

				if (currentNode != null)
				{
					if (currentNode.source_target_type.equalsIgnoreCase("target") && downLevel > 0)
					{
						node.id = nodes.size();
						nodes.add(node);
						addedJobNodes.put(jobExecId, node.id);
						LineageEdge edge = new LineageEdge();
						edge.source = currentNode.id;
						edge.target = node.id;
						edge.id = edges.size();
						edge.label = (String) row.get("operation");
						edge.chain = (String) row.get("flow_path");
						edges.add(edge);
					}
					else if (currentNode.source_target_type.equalsIgnoreCase("source") && upLevel > 0)
					{
						node.id = nodes.size();
						nodes.add(node);
						addedJobNodes.put(jobExecId, node.id);
						LineageEdge edge = new LineageEdge();
						edge.source = node.id;
						edge.target = currentNode.id;
						edge.id = edges.size();
						edge.label = (String) row.get("operation");
						edge.chain = (String) row.get("flow_path");
						edges.add(edge);
					}
					else
					{
						continue;
					}
				}
				else
				{
					node.id = nodes.size();
					nodes.add(node);
					addedJobNodes.put(jobExecId, node.id);
				}
				int jobIndex = node.id;
				int applicationID = (int)row.get("app_id");
				Long jobId = ((BigInteger)row.get("job_exec_id")).longValue();
				List<Map<String, Object>> relatedDataRows = null;
				if (currentNode != null)
				{
					relatedDataRows = getJdbcTemplate().queryForList(
							GET_DATA_WITH_SOURCE,
							applicationID,
							jobId,
							currentNode.source_target_type);
				}
				else
				{
					relatedDataRows = getJdbcTemplate().queryForList(
							GET_DATA,
							applicationID,
							jobId);
				}

				if (relatedDataRows != null) {
					for (Map relatedDataRow : relatedDataRows)
					{
						String abstractedObjectName = (String)relatedDataRow.get("abstracted_object_name");
						if (abstractedObjectName.startsWith("/tmp/"))
						{
							continue;
						}
						String relatedSourceType = (String)relatedDataRow.get("source_target_type");
						if (relatedSourceType.equalsIgnoreCase("source"))
						{
							LineageNode relatedNode = new LineageNode();
							relatedNode._sort_list = new ArrayList<String>();
							relatedNode.node_type = "data";
							relatedNode.source_target_type = relatedSourceType;
							relatedNode.abstracted_path = (String)relatedDataRow.get("abstracted_object_name");
							relatedNode.storage_type = ((String)relatedDataRow.get("storage_type")).toLowerCase();
							relatedNode.job_start_unix_time = (Long)relatedDataRow.get("job_start_unix_time");
							relatedNode.job_end_unix_time = (Long)relatedDataRow.get("job_end_unix_time");
							LineagePathInfo info = new LineagePathInfo();
							info.filePath = relatedNode.abstracted_path;
							info.storageType = relatedNode.storage_type;
							relatedNode.urn = utils.Lineage.convertToURN(info);
							relatedNode._sort_list.add("abstracted_path");
							relatedNode._sort_list.add("storage_type");
							relatedNode._sort_list.add("urn");
							List<LineageNode> addedTargets = addedTargetNodes.get(abstractedObjectName);
							LineageNode addedSource = addedSourceNode.get(abstractedObjectName);
							LineageNode existNode = null;
							if (addedTargets != null)
							{
								Collections.sort(addedTargets, new Comparator<LineageNode>(){
									public int compare(LineageNode node1, LineageNode node2){
										if(node1.job_end_unix_time == node2.job_end_unix_time)
											return 0;
										return node1.job_end_unix_time > node2.job_end_unix_time ? -1 : 1;
									}
								});
								for(LineageNode target : addedTargets)
								{
									if (relatedNode.job_start_unix_time != null
											&& target.job_end_unix_time != null
											&& relatedNode.job_start_unix_time > target.job_end_unix_time)
									{
										existNode = target;
										break;
									}
								}
							}
							if (existNode == null && addedSource != null)
							{
								existNode = addedSource;
							}
							if (existNode != null)
							{
								relatedNode.id = existNode.id;
							}
							else
							{
								relatedNode.id = nodes.size();
								nodes.add(relatedNode);
								addedSourceNode.put(abstractedObjectName, relatedNode);
							}
							LineageEdge relatedEdge = new LineageEdge();
							relatedEdge.source = relatedNode.id;
							relatedEdge.target = jobIndex;
							relatedEdge.id = edges.size();
							relatedEdge.label = (String)relatedDataRow.get("operation");
							relatedEdge.chain = "";
							edges.add(relatedEdge);
							if (upLevel > 1)
							{
								LineagePathInfo subPath = new LineagePathInfo();
								subPath.storageType = relatedNode.storage_type;
								subPath.filePath = relatedNode.abstracted_path;
								getObjectAdjacentNode(
										subPath,
										upLevel - 1,
										0,
										relatedNode,
										nodes,
										edges,
										addedSourceNode,
										addedTargetNodes,
										addedJobNodes,
										lookBackTime);
							}
						}
						else if (relatedSourceType.equalsIgnoreCase("target"))
						{
							List<LineageNode> addedTargets = addedTargetNodes.get(abstractedObjectName);
							LineageNode relatedNode = new LineageNode();
							relatedNode._sort_list = new ArrayList<String>();
							relatedNode.node_type = "data";
							relatedNode.source_target_type = relatedSourceType;
							relatedNode.abstracted_path = (String)relatedDataRow.get("abstracted_object_name");
							relatedNode.storage_type = ((String)relatedDataRow.get("storage_type")).toLowerCase();
							LineagePathInfo info = new LineagePathInfo();
							info.filePath = relatedNode.abstracted_path;
							info.storageType = relatedNode.storage_type;
							relatedNode.urn = utils.Lineage.convertToURN(info);
							relatedNode._sort_list.add("abstracted_path");
							relatedNode._sort_list.add("storage_type");
							LineageNode existNode = null;
							if (addedTargets != null)
							{
								for(LineageNode target : addedTargets)
								{
									if (relatedNode.partition_end != null )
									{
										if (relatedNode.partition_end == target.partition_end)
										{
											existNode = target;
											break;
										}
									}
									else
									{
										if (relatedNode.job_end_unix_time == target.job_end_unix_time)
										{
											existNode = target;
											break;
										}

									}

								}
							}
							if (existNode != null)
							{
								relatedNode.id = existNode.id;
							}
							else
							{
								relatedNode.id = nodes.size();
								nodes.add(relatedNode);
								if (addedTargets == null)
								{
									addedTargets = new ArrayList<LineageNode>();
								}
								addedTargets.add(relatedNode);
								addedTargetNodes.put(abstractedObjectName, addedTargets);
							}

							LineageEdge relatedEdge = new LineageEdge();
							relatedEdge.source = jobIndex;
							relatedEdge.target = relatedNode.id;
							relatedEdge.id = edges.size();
							relatedEdge.label = (String)relatedDataRow.get("operation");
							relatedEdge.chain = "";
							edges.add(relatedEdge);
							if (downLevel > 1)
							{
								LineagePathInfo subPath = new LineagePathInfo();
								subPath.storageType = relatedNode.storage_type;
								subPath.filePath = relatedNode.abstracted_path;
								getObjectAdjacentNode(
										subPath,
										0,
										downLevel - 1,
										relatedNode,
										nodes,
										edges,
										addedSourceNode,
										addedTargetNodes,
										addedJobNodes,
										lookBackTime);
							}
						}
					}

				}
			}
		}
	}

	public static void getObjectAdjacentNode(
			LineagePathInfo pathInfo,
			int upLevel,
			int downLevel,
			LineageNode currentNode,
			List<LineageNode> nodes,
			List<LineageEdge> edges,
			Map<String, LineageNode> addedSourceNode,
			Map<String, List<LineageNode>> addedTargetNodes,
			Map<Long, Integer> addedJobNodes,
			int lookBackTime)
	{
		if (upLevel < 1 && downLevel < 1)
		{
			return;
		}

		if (pathInfo == null || StringUtils.isBlank(pathInfo.filePath))
		{
			return;
		}
		searchInAzkaban(
				pathInfo,
				upLevel,
				downLevel,
				currentNode,
				nodes,
				edges,
				addedSourceNode,
				addedTargetNodes,
				addedJobNodes,
				lookBackTime);
	}

	public static ObjectNode getFlowLineage(String application, String project, Long flowId)
	{
		ObjectNode resultNode = Json.newObject();
		List<LineageNode> nodes = new ArrayList<LineageNode>();
		List<LineageEdge> edges = new ArrayList<LineageEdge>();
		String flowName = null;

		Map<Long, Integer> addedJobNodes = new HashMap<Long, Integer>();
		Map<Pair, Integer> addedDataNodes = new HashMap<Pair, Integer>();

		if (StringUtils.isBlank(application) || StringUtils.isBlank(project) || (flowId <= 0))
		{
			resultNode.set("nodes", Json.toJson(nodes));
			resultNode.set("links", Json.toJson(edges));
			return resultNode;
		}

		String applicationName = application.replace(".", " ");

		int appID = 0;
		try
		{
			appID = getJdbcTemplate().queryForObject(
					GET_APP_ID,
					new Object[] {applicationName},
					Integer.class);
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("getFlowLineage get application id failed, application name = " + application);
			Logger.error("Exception = " + e.getMessage());
		}

		Map<Long, List<LineageNode>> nodeHash = new HashMap<Long, List<LineageNode>>();
		Map<String, List<LineageNode>> partitionedNodeHash = new HashMap<String, List<LineageNode>>();

		if (appID != 0)
		{
			try
			{
				flowName = getJdbcTemplate().queryForObject(
						GET_FLOW_NAME,
						new Object[] {appID, flowId},
						String.class);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("getFlowLineage get flow name failed, application name = " + application +
						" flowId " + Long.toString(flowId));
				Logger.error("Exception = " + e.getMessage());
			}

			Long flowExecId = 0L;
			try
			{
				flowExecId = getJdbcTemplate().queryForObject(
						GET_LATEST_FLOW_EXEC_ID,
						new Object[] {appID, flowId},
						Long.class);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("getFlowLineage get flow execution id failed, application name = " + application +
						" flowId " + Long.toString(flowExecId));
				Logger.error("Exception = " + e.getMessage());
			}
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(
					GET_FLOW_DATA_LINEAGE,
					appID,
					flowExecId);
			if (rows != null)
			{
				for (Map row : rows) {
					Long jobExecId = ((BigInteger)row.get("job_exec_id")).longValue();
					LineageNode node = new LineageNode();
					node.abstracted_path = (String)row.get("abstracted_object_name");
					node.source_target_type = (String)row.get("source_target_type");
					node.exec_id = jobExecId;
					Object recordCountObject = row.get("record_count");
					if (recordCountObject != null)
					{
						node.record_count = ((BigInteger)recordCountObject).longValue();
					}

					node.application_id = (int)row.get("app_id");
					node.cluster = (String)row.get("app_code");
					node.partition_type = (String)row.get("partition_type");
					node.operation = (String)row.get("operation");
					node.partition_start = (String)row.get("partition_start");
					node.partition_end = (String)row.get("partition_end");
					node.full_object_name = (String)row.get("full_object_name");
					node.job_start_time = row.get("start_time").toString();
					node.job_end_time = row.get("end_time").toString();
					node.storage_type = ((String)row.get("storage_type")).toLowerCase();
					node.node_type = "data";
					node._sort_list = new ArrayList<String>();
					node._sort_list.add("cluster");
					node._sort_list.add("abstracted_path");
					node._sort_list.add("storage_type");
					node._sort_list.add("partition_type");
					node._sort_list.add("partition_start");
					node._sort_list.add("partition_end");
					node._sort_list.add("source_target_type");
					List<LineageNode> nodeList = nodeHash.get(jobExecId);
					if (nodeList != null)
					{
						nodeList.add(node);
					}
					else
					{
						nodeList = new ArrayList<LineageNode>();
						nodeList.add(node);
						nodeHash.put(jobExecId, nodeList);
					}
				}
			}

			List<LineageNode> jobNodes = new ArrayList<LineageNode>();
			List<Map<String, Object>> jobRows = null;
			jobRows = getJdbcTemplate().queryForList(
					GET_FLOW_JOB,
					appID,
					flowExecId,
					30);
			int index = 0;
			int edgeIndex = 0;
			Map<Long, LineageNode> jobNodeMap = new HashMap<Long, LineageNode>();
			List<Pair> addedEdges = new ArrayList<Pair>();
			if (rows != null)
			{
				for (Map row : jobRows) {
					Long jobExecId = ((BigInteger)row.get("job_exec_id")).longValue();
					LineageNode node = new LineageNode();
					node._sort_list = new ArrayList<String>();
					node.node_type = "script";
					node.job_type = (String)row.get("job_type");
					node.cluster = (String)row.get("app_code");
					node.job_path = (String)row.get("job_path");
					node.job_name = (String)row.get("job_name");
					node.pre_jobs = (String)row.get("pre_jobs");
					node.post_jobs = (String)row.get("post_jobs");
					node.job_id = (Long)row.get("job_id");
					node.job_start_time = row.get("start_time").toString();
					node.job_end_time = row.get("end_time").toString();
					node.exec_id = jobExecId;
					node._sort_list.add("cluster");
					node._sort_list.add("job_path");
					node._sort_list.add("job_name");
					node._sort_list.add("job_type");
					node._sort_list.add("job_start_time");
					node._sort_list.add("job_end_time");
					Integer id = addedJobNodes.get(jobExecId);
					if (id == null)
					{
						node.id = index++;
						nodes.add(node);
						jobNodeMap.put(node.job_id, node);
						jobNodes.add(node);
						addedJobNodes.put(jobExecId, node.id);
					}
					else
					{
						node.id = id;
					}

					String sourceType = (String)row.get("source_target_type");
					if (sourceType.equalsIgnoreCase("target"))
					{
						List<LineageNode> sourceNodeList = nodeHash.get(jobExecId);
						if (sourceNodeList != null && sourceNodeList.size() > 0)
						{
							for(LineageNode sourceNode : sourceNodeList)
							{
								if (sourceNode.source_target_type.equalsIgnoreCase("source"))
								{
									Pair matchedSourcePair = new ImmutablePair<>(
											sourceNode.abstracted_path,
											sourceNode.partition_end);
									Integer nodeId = addedDataNodes.get(matchedSourcePair);
									if (nodeId == null)
									{
										List<LineageNode> nodeList = partitionedNodeHash.get(sourceNode.abstracted_path);
										if (StringUtils.isBlank(sourceNode.partition_end))
										{
											Boolean bFound = false;
											if (nodeList != null)
											{
												for(LineageNode n : nodeList)
												{
													if (StringUtils.isNotBlank(n.partition_end) &&
															n.partition_end.compareTo(sourceNode.job_start_time) < 0)
													{
														sourceNode.id = n.id;
														bFound = true;
														break;
													}
												}
											}
											if (!bFound)
											{
												sourceNode.id = index++;
												nodes.add(sourceNode);
												Pair sourcePair = new ImmutablePair<>(
														sourceNode.abstracted_path,
														sourceNode.partition_end);
												addedDataNodes.put(sourcePair, sourceNode.id);
											}
										}
										else
										{
											if (nodeList == null)
											{
												nodeList = new ArrayList<LineageNode>();
											}
											nodeList.add(sourceNode);
											partitionedNodeHash.put(sourceNode.abstracted_path, nodeList);
											sourceNode.id = index++;
											nodes.add(sourceNode);
											Pair sourcePair = new ImmutablePair<>(
													sourceNode.abstracted_path,
													sourceNode.partition_end);
											addedDataNodes.put(sourcePair, sourceNode.id);
										}
									}
									else
									{
										sourceNode.id = nodeId;
									}
									LineageEdge edge = new LineageEdge();
									edge.id = edgeIndex++;
									edge.source = sourceNode.id;
									edge.target = node.id;
									if (StringUtils.isNotBlank(sourceNode.operation))
									{
										edge.label = sourceNode.operation;
									}
									else
									{
										edge.label = "load";
									}
									edge.chain = "data";
									edges.add(edge);
								}
							}
						}
					}
					else if (sourceType.equalsIgnoreCase("source"))
					{

						List<LineageNode> targetNodeList = nodeHash.get(jobExecId);
						if (targetNodeList != null && targetNodeList.size() > 0)
						{
							for(LineageNode targetNode : targetNodeList)
							{
								if (targetNode.source_target_type.equalsIgnoreCase("target"))
								{
									Pair matchedTargetPair = new ImmutablePair<>(
											targetNode.abstracted_path,
											targetNode.partition_end);
									Integer nodeId = addedDataNodes.get(matchedTargetPair);
									if (nodeId == null)
									{
										List<LineageNode> nodeList = partitionedNodeHash.get(targetNode.abstracted_path);
										if (StringUtils.isBlank(targetNode.partition_end))
										{
											Boolean bFound = false;
											if (nodeList != null)
											{
												for(LineageNode n : nodeList)
												{
													if (StringUtils.isNotBlank(n.partition_end) &&
															n.partition_end.compareTo(targetNode.job_start_time) < 0)
													{
														targetNode.id = n.id;
														bFound = true;
														break;
													}
												}
											}
											if (!bFound)
											{
												targetNode.id = index++;
												nodes.add(targetNode);
												Pair targetPair = new ImmutablePair<>(
														targetNode.abstracted_path,
														targetNode.partition_end);
												addedDataNodes.put(targetPair, targetNode.id);
											}
										}
										else
										{
											if (nodeList == null)
											{
												nodeList = new ArrayList<LineageNode>();
											}
											nodeList.add(targetNode);
											partitionedNodeHash.put(targetNode.abstracted_path, nodeList);
											targetNode.id = index++;
											nodes.add(targetNode);
											Pair targetPair = new ImmutablePair<>(
													targetNode.abstracted_path,
													targetNode.partition_end);
											addedDataNodes.put(targetPair, targetNode.id);
										}
									}
									else
									{
										targetNode.id = nodeId;
									}
									LineageEdge edge = new LineageEdge();
									edge.id = edgeIndex++;
									edge.source = node.id;
									edge.target = targetNode.id;
									if (StringUtils.isNotBlank(targetNode.operation))
									{
										edge.label = targetNode.operation;
									}
									else
									{
										edge.label = "load";
									}
									edge.chain = "data";
									edges.add(edge);
								}
							}
						}
					}
				}
				for (LineageNode node : jobNodes)
				{
					Long jobId = node.job_id;
					if (StringUtils.isNotBlank(node.pre_jobs))
					{
						String [] prevJobIds = node.pre_jobs.split(",");
						if (prevJobIds != null)
						{
							for(String jobIdString: prevJobIds)
							{
								if(StringUtils.isNotBlank(jobIdString))
								{
									Long id = Long.parseLong(jobIdString);
									LineageNode sourceNode = jobNodeMap.get(id);
									if (sourceNode != null)
									{
										Pair pair = new ImmutablePair<>(sourceNode.id, node.id);
										if (!addedEdges.contains(pair))
										{
											LineageEdge edge = new LineageEdge();
											edge.id = edgeIndex++;
											edge.source = sourceNode.id;
											edge.target = node.id;
											edge.label = "";
											edge.type = "job";
											edges.add(edge);
											addedEdges.add(pair);
										}
									}
								}
							}
						}
					}

					if (StringUtils.isNotBlank(node.post_jobs))
					{
						String [] postJobIds = node.post_jobs.split(",");
						if (postJobIds != null)
						{
							for(String jobIdString: postJobIds)
							{
								if(StringUtils.isNotBlank(jobIdString))
								{
									Long id = Long.parseLong(jobIdString);
									LineageNode targetNode = jobNodeMap.get(id);
									if (targetNode != null)
									{
										Pair pair = new ImmutablePair<>(node.id, targetNode.id);
										if (!addedEdges.contains(pair))
										{
											LineageEdge edge = new LineageEdge();
											edge.id = edgeIndex++;
											edge.source = node.id;
											edge.target = targetNode.id;
											edge.label = "";
											edge.type = "job";
											edges.add(edge);
											addedEdges.add(pair);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		resultNode.set("nodes", Json.toJson(nodes));
		resultNode.set("links", Json.toJson(edges));
		resultNode.put("flowName", flowName);
		return resultNode;
	}

	public static void getImpactDatasets(
			List<String> searchUrnList,
			int level,
			List<ImpactDataset> impactDatasets)
	{
		if (searchUrnList != null && searchUrnList.size() > 0) {

			if (impactDatasets == null)
			{
				impactDatasets = new ArrayList<ImpactDataset>();
			}

			List<String> pathList = new ArrayList<String>();
			List<String> nextSearchList = new ArrayList<String>();

			for (String urn : searchUrnList)
			{
				LineagePathInfo pathInfo = Lineage.convertFromURN(urn);
				if (pathInfo != null && StringUtils.isNotBlank(pathInfo.filePath))
				{
					if (!pathList.contains(pathInfo.filePath))
					{
						pathList.add(pathInfo.filePath);
					}
				}
			}

			if (pathList != null && pathList.size() > 0)
			{
				Map<String, List> param = Collections.singletonMap("pathlist", pathList);
				NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
						NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
				List<ImpactDataset> impactDatasetList = namedParameterJdbcTemplate.query(
						GET_ONE_LEVEL_IMPACT_DATABASES,
						param,
						new ImpactDatasetRowMapper());

				if (impactDatasetList != null) {
					for (ImpactDataset dataset : impactDatasetList) {
						dataset.level = level;
						if (impactDatasets.stream().filter(o -> o.urn.equals(dataset.urn)).findFirst().isPresent())
						{
							continue;
						}
						impactDatasets.add(dataset);
						nextSearchList.add(dataset.urn);
					}
				}
			}

			if (nextSearchList.size() > 0)
			{
				getImpactDatasets(nextSearchList, level + 1, impactDatasets);
			}
		}
	}

	public static List<ImpactDataset> getImpactDatasetsByUrn(String urn)
	{
		List<ImpactDataset> impactDatasetList = new ArrayList<ImpactDataset>();

		if (StringUtils.isNotBlank(urn))
		{
			List<String> searchUrnList = new ArrayList<String>();
			searchUrnList.add(urn);
			getImpactDatasets(searchUrnList, 1, impactDatasetList);
		}

		return impactDatasetList;
	}

}
